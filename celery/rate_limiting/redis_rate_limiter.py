"""Redis-backed *global* token bucket for Celery task rate limiting.

This module isolates **all** logic for the opt-in, Redis-backed global rate
limiter. By default Celery enforces a task's ``rate_limit`` *per worker
process*: every :class:`~celery.worker.consumer.consumer.Consumer` builds its
own in-memory :class:`kombu.utils.limits.TokenBucket` through
``bucket_for_task()``. Consequently a task declared at ``"10/s"`` running on
ten workers may execute at an aggregate of up to ``100/s``.

When the operator sets the opt-in setting ``task_global_rate_limit_backend``
(legacy alias ``CELERY_GLOBAL_RATE_LIMIT_BACKEND``) to a Redis URL such as
``redis://localhost:6379/0``, the consumer's ``bucket_for_task()`` factory
returns the :class:`RedisTokenBucket` defined here instead of the per-worker
bucket. ``RedisTokenBucket`` consults Redis atomically so the configured limit
holds **globally across the entire worker fleet**. When the setting is unset
this module is never instantiated and behaviour is byte-for-byte identical to
today's per-worker bucket.

Design notes
------------
* :class:`RedisTokenBucket` subclasses :class:`kombu.utils.limits.TokenBucket`
  and overrides **only** :meth:`~RedisTokenBucket.can_consume` and
  :meth:`~RedisTokenBucket.expected_time`. The per-worker pending-queue
  machinery (``add``/``pop``/``contents``/``clear_pending``) is inherited
  unchanged so the consumer's scheduling loop is unaffected.
* Token accounting runs in an atomic server-side Lua script so the
  read-refill-decide-write cycle cannot be interrupted; two workers can never
  double-spend the same allowance. A single key per task name keeps the script
  Redis Cluster safe and lets distinct tasks/apps coexist without collisions.
* The Redis connection is **independent** of the broker / ``result_backend``;
  it is opened lazily from the configured URL so an unreachable Redis at worker
  startup does not crash bootstrap.
* On a Redis *server* error the limiter degrades explicitly and configurably:
  fail-open (allow, the default) or fail-closed (block) per
  ``task_global_rate_limit_fail_open``. A *misconfiguration* (backend set but
  ``redis-py`` not installed) instead raises
  :class:`~celery.exceptions.ImproperlyConfigured` loudly.

The ``redis-py`` import is **guarded** (mirroring ``celery/backends/redis.py``)
so this module stays importable even when ``redis-py`` is absent -- it is not a
core dependency, only available transitively through the ``kombu[redis]``
extra. The rate-limit syntax (``"10/s"``/``"100/m"``/``"2/h"``) is parsed
elsewhere by ``celery/utils/time.py``'s ``rate()``; this module receives an
already-parsed tokens/second float and never re-parses it.
"""
from __future__ import annotations

from math import ceil

from kombu.utils.limits import TokenBucket
from kombu.utils.url import maybe_sanitize_url

from celery.exceptions import ImproperlyConfigured
from celery.utils.log import get_logger

try:
    import redis
    from redis.exceptions import RedisError
except ImportError:  # pragma: no cover
    redis = None

    class RedisError(Exception):
        """Fallback so ``except RedisError`` is valid when redis-py is absent.

        ``redis-py`` is not a core Celery dependency (it is provided only via
        the ``kombu[redis]`` extra). Defining a stand-in keeps this module
        importable on installations without it; any actual *use* of the limiter
        without ``redis-py`` raises :class:`ImproperlyConfigured` instead.
        """

logger = get_logger(__name__)

__all__ = ('RedisTokenBucket',)

#: Prefix for every per-task Redis key. The full key is ``KEY_PREFIX +
#: task_name`` so distinct tasks (and distinct applications sharing one Redis
#: instance) never collide on rate-limit state.
KEY_PREFIX = 'celery:global-rate-limit:'

#: Lower bound (seconds) for the per-task key TTL. Every key carries an expiry
#: so stale rate-limit state self-cleans once a task stops being produced.
MIN_KEY_TTL = 60

#: Message raised as :class:`ImproperlyConfigured` when the global backend is
#: configured but ``redis-py`` is not installed -- a loud misconfiguration,
#: deliberately distinct from the fail-open/fail-closed degradation path which
#: handles Redis *server* errors only. Mirrors ``celery/backends/redis.py``.
E_REDIS_MISSING = """
You configured task_global_rate_limit_backend but the redis library is not \
installed. Install it (for example via the celery[redis] extra) to use the \
global rate limiter, or unset the setting to fall back to per-worker limits.
"""

#: Message raised as :class:`ImproperlyConfigured` when the configured backend
#: URL is malformed -- ``redis.Redis.from_url`` rejects it with ``ValueError``.
#: Like the missing-``redis-py`` case above this is a *misconfiguration* that no
#: amount of retrying will fix, so it is surfaced loudly and is deliberately
#: distinct from the fail-open/fail-closed path (which handles transient Redis
#: *server* errors only). The ``%s`` is filled with the **sanitised** URL so any
#: credentials embedded in the setting are never surfaced in the error.
E_REDIS_BAD_URL = """
The task_global_rate_limit_backend setting is not a valid Redis URL: %s. It \
must use a redis://, rediss:// or unix:// scheme (for example \
redis://localhost:6379/0). Fix the setting to a valid Redis URL, or unset it \
to fall back to per-worker rate limits.
"""

#: Atomic token-bucket *consume* script. Refills the bucket from the Redis
#: server clock (so all workers share one authoritative time source), then
#: consumes ``requested`` tokens when available. Returns ``1`` when the request
#: is allowed and ``0`` when it is denied. The whole read-refill-decide-write
#: cycle runs atomically on the server, so concurrent workers cannot
#: double-spend the same allowance. It touches a single key -> Cluster safe.
TOKEN_BUCKET_LUA = """
redis.replicate_commands()
local capacity = tonumber(ARGV[1])
local fill_rate = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local t = redis.call('TIME')
local now = tonumber(t[1]) + (tonumber(t[2]) / 1000000)
local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then tokens = capacity; ts = now end
local delta = now - ts
if delta < 0 then delta = 0 end
tokens = math.min(capacity, tokens + (delta * fill_rate))
local allowed = 0
if tokens >= requested then tokens = tokens - requested; allowed = 1 end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('EXPIRE', KEYS[1], ttl)
return allowed
"""

#: Read-only companion to :data:`TOKEN_BUCKET_LUA`. Computes how long (in whole
#: microseconds) until ``requested`` tokens become available, without mutating
#: the bucket. Microseconds are returned because Lua truncates floating-point
#: return values to integers; the Python caller divides back to seconds.
EXPECTED_TIME_LUA = """
redis.replicate_commands()
local capacity = tonumber(ARGV[1])
local fill_rate = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local t = redis.call('TIME')
local now = tonumber(t[1]) + (tonumber(t[2]) / 1000000)
local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then tokens = capacity; ts = now end
local delta = now - ts
if delta < 0 then delta = 0 end
tokens = math.min(capacity, tokens + (delta * fill_rate))
local deficit = requested - tokens
if deficit <= 0 then return 0 end
return math.ceil((deficit / fill_rate) * 1000000)
"""


class RedisTokenBucket(TokenBucket):
    """Redis-backed, :class:`~kombu.utils.limits.TokenBucket`-compatible bucket.

    A drop-in replacement for the per-worker ``TokenBucket`` that enforces a
    task's ``rate_limit`` **globally across the whole worker fleet** by keeping
    the token state in Redis. It is instantiated solely by the consumer's
    ``bucket_for_task()`` factory when ``task_global_rate_limit_backend`` is
    configured.

    Only :meth:`can_consume` and :meth:`expected_time` are overridden; every
    other part of the interface the consumer relies on -- :meth:`add`,
    :meth:`pop`, the :attr:`contents` deque and :meth:`clear_pending` -- is
    inherited unchanged so the local pending queue stays per-worker and the
    consumer's scheduling loop is unaffected.

    Arguments:
        fill_rate (float): Refill rate in **tokens/second**, already parsed from
            the task's ``rate_limit`` by ``celery.utils.time.rate()``
            (``"10/s"`` -> ``10.0``). Passed straight to the base class and
            never re-parsed here.
        capacity (float): Maximum number of tokens in the bucket. The consumer
            passes ``1`` (matching today's per-worker bucket). Passed to the
            base class.

    Keyword Arguments:
        backend_url (str): Redis URL from ``task_global_rate_limit_backend``
            (for example ``redis://localhost:6379/0``). Used to open an
            **independent** connection -- never the broker / ``result_backend``.
        task_name (str): The task's ``name``, used to namespace the per-task
            Redis key as ``celery:global-rate-limit:<task_name>``.
        fail_open (bool): Degradation mode on a Redis *server* error. ``True``
            (default) allows the task (fail-open) so a coordination-layer
            outage never halts processing; ``False`` blocks it (fail-closed).

    Notes:
        The Redis connection is created lazily on first use, so an unreachable
        Redis at worker startup does not crash bootstrap. Token accounting is
        performed by an atomic Lua script (:data:`TOKEN_BUCKET_LUA`); see the
        module docstring and ``celery/backends/redis.py`` for the connection
        precedent.
    """

    def __init__(self, fill_rate, capacity=1, *,
                 backend_url=None, task_name=None, fail_open=True):
        # Initialise the inherited token-bucket state (fill_rate, capacity,
        # _tokens, timestamp and the ``contents`` deque). ``fill_rate`` is
        # already in tokens/second -- do NOT re-parse the rate string.
        super().__init__(fill_rate, capacity)
        self._backend_url = backend_url
        self._task_name = task_name
        self._fail_open = bool(fail_open)
        # Per-task key namespacing prevents cross-task / cross-application
        # collisions when a single Redis instance is shared.
        self._key = KEY_PREFIX + (task_name or '')
        # Every key MUST expire so abandoned tasks do not leak state. Size the
        # TTL to comfortably exceed one full refill window. ``fill_rate`` is
        # always > 0 because the consumer only builds a bucket for a truthy
        # rate_limit, so this division is safe.
        self._ttl = max(
            MIN_KEY_TTL,
            int(ceil(self.capacity / self.fill_rate)) + MIN_KEY_TTL,
        )
        # Connection and registered Lua scripts are created lazily in
        # _get_client(); constructing the bucket performs NO Redis access.
        self._redis_client = None
        self._consume_script = None
        self._expected_script = None

    def _get_client(self):
        """Return the lazily-created, cached Redis client.

        The client is built on first use via ``redis.Redis.from_url`` (which
        opens no socket until the first command) and the two Lua scripts are
        registered on it (a local-only operation that does not contact Redis).
        Two *misconfigurations* are surfaced loudly as
        :class:`~celery.exceptions.ImproperlyConfigured` (each distinct from the
        fail-open/fail-closed path, which handles only transient Redis *server*
        errors): the global backend being configured while ``redis-py`` is not
        installed, and the configured backend URL being malformed (rejected by
        ``redis.Redis.from_url`` with ``ValueError``).
        """
        if self._redis_client is None:
            if redis is None:
                raise ImproperlyConfigured(E_REDIS_MISSING.strip())
            # Independent connection from the configured URL -- NOT the broker
            # or result_backend connection. A malformed URL is a permanent
            # misconfiguration (not a transient server outage), so convert the
            # raw redis-py ValueError into a loud ImproperlyConfigured -- mirroring
            # the missing-redis-py branch above -- instead of silently
            # fail-open/closed, which would hide the bad setting. The URL is
            # sanitised so any embedded credentials never leak into the error.
            try:
                self._redis_client = redis.Redis.from_url(self._backend_url)
            except ValueError as exc:
                raise ImproperlyConfigured(
                    E_REDIS_BAD_URL.strip()
                    % maybe_sanitize_url(self._backend_url)
                ) from exc
            # ``register_script`` does not contact Redis; the EVALSHA round trip
            # happens only when the returned Script is invoked.
            self._consume_script = self._redis_client.register_script(
                TOKEN_BUCKET_LUA)
            self._expected_script = self._redis_client.register_script(
                EXPECTED_TIME_LUA)
        return self._redis_client

    def can_consume(self, tokens=1):
        """Atomically attempt to consume ``tokens`` from the *global* bucket.

        Runs the atomic :data:`TOKEN_BUCKET_LUA` script against the per-task
        key so concurrent workers cannot double-spend the shared allowance.

        Returns:
            bool: ``True`` if the tokens were consumed (the task may run),
                ``False`` otherwise. On a Redis *server* error the result is the
                configured degradation mode (:attr:`_fail_open`).
        """
        try:
            self._get_client()
            allowed = self._consume_script(
                keys=[self._key],
                args=[self.capacity, self.fill_rate, tokens, self._ttl],
            )
            # Lua returns an integer 1/0; coerce defensively in case the client
            # surfaces it as a byte/str before deciding allow vs. deny.
            return bool(int(allowed))
        except RedisError as exc:
            # Explicit, configurable degradation: never let a coordination-layer
            # outage raise into the consumer loop. The URL is sanitised so any
            # embedded credentials are not logged in plaintext.
            logger.warning(
                'Global rate limiter degraded for task %r (backend=%s): %r; '
                'failing %s', self._task_name,
                maybe_sanitize_url(self._backend_url), exc,
                'open' if self._fail_open else 'closed')
            return self._fail_open

    def expected_time(self, tokens=1):
        """Return seconds until ``tokens`` are expected to be available.

        Consulted by the consumer only after :meth:`can_consume` returns
        ``False``, to schedule a re-check. Returns ``0.0`` when the tokens are
        already available. On a Redis *server* error it falls back to a simple
        ``tokens / fill_rate`` backoff so the fail-closed retry path stays
        bounded rather than raising.
        """
        try:
            self._get_client()
            micros = self._expected_script(
                keys=[self._key],
                args=[self.capacity, self.fill_rate, tokens],
            )
            # The script returns whole microseconds (Lua truncates floats);
            # divide back to seconds and clamp to a non-negative hold.
            return max(0.0, float(micros) / 1000000.0)
        except RedisError as exc:
            logger.warning(
                'Global rate limiter expected_time degraded for task %r '
                '(backend=%s): %r', self._task_name,
                maybe_sanitize_url(self._backend_url), exc)
            # fill_rate is always > 0 (a bucket is only built for a truthy
            # rate_limit), so this fallback backoff is finite.
            return tokens / self.fill_rate
