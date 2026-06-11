"""Redis-backed global (cross-worker) token bucket rate limiter.

This module provides :class:`GlobalTokenBucket`, a drop-in subclass of
:class:`kombu.utils.limits.TokenBucket` whose token state is stored in Redis
under a per-task key instead of in process-local memory.  Because every worker
process and node draws tokens from the same shared key, a task's configured
``rate_limit`` (for example ``"10/s"``) becomes a single aggregate ceiling for
the whole worker pool rather than being enforced independently inside each
worker process.

Token consumption runs through an atomic server-side Lua script, so the
refill/check/consume cycle happens in one indivisible step on Redis.  This
closes the read-outside-transaction race window that a naive read-then-write
(or ``WATCH``/``MULTI``/``EXEC``) approach would leave open, guaranteeing that
two workers racing for the last token cannot both succeed -- the "locks handled
appropriately to address race conditions" requirement, satisfied without a
client-side lock.

When Redis is missing (the optional ``celery[redis]`` extra is not installed)
or unreachable, the bucket transparently falls back to the inherited local
token-bucket math, so a worker keeps running -- merely degraded to the default
per-worker limiting -- instead of failing.
"""
import time

from kombu.utils.limits import TokenBucket

try:
    import redis
except ImportError:  # pragma: no cover
    redis = None  # redis is an optional extra (``celery[redis]``).

__all__ = ('GlobalTokenBucket',)


# Default socket timeouts (seconds) applied when building the per-bucket Redis
# client from a URL.  ``socket_connect_timeout`` bounds the TCP connect phase
# and ``socket_timeout`` bounds every read/write.  Without them, a black-hole /
# unreachable Redis (cloud failover, network partition, firewall drop) would
# make every ``can_consume`` admission call block on the OS TCP connect timeout
# (~127s on Linux) before the graceful-fallback ``except`` fires -- freezing the
# worker's admission hot path during an outage instead of degrading quickly.
# A small default keeps the Redis-down fallback fast/bounded; the healthy hot
# path is sub-millisecond so this ceiling is never hit in normal operation.
# Operators who need a different bound can override it through the
# ``worker_global_rate_limit_url`` query string (for example
# ``redis://host:6379/0?socket_connect_timeout=5``), which takes precedence over
# this default -- so no extra user-facing setting is introduced.
DEFAULT_SOCKET_TIMEOUT = 2.0


# -- Atomic token-bucket Lua script ------------------------------------------
# Executed server-side by Redis (via EVALSHA, with automatic EVAL fallback) so
# the whole refill -> check -> consume -> write cycle is a single atomic
# operation.  This is what makes cross-worker consumption race-free WITHOUT any
# client-side lock: there is no window between reading the token count and
# writing it back in which another worker could observe stale state.
#
#   KEYS[1]            per-task bucket key (namespaced per task name).
#   ARGV[1] fill_rate  tokens added to the bucket per second.
#   ARGV[2] capacity   maximum number of tokens the bucket can hold.
#   ARGV[3] requested  number of tokens this call wants to consume.
#   ARGV[4] now        caller's epoch time in seconds (float); passed in so the
#                      refill clock is shared across processes and testable.
#
# Returns a two-element table ``{allowed, wait_ms}`` where ``allowed`` is 1/0
# and ``wait_ms`` is the integer number of milliseconds until ``requested``
# tokens would be available.  The wait is returned in *integer* milliseconds
# because redis-py converts a Lua number reply to a Python ``int`` (truncating
# any fractional part); the Python side divides by 1000 to recover seconds.
LUA_TOKEN_BUCKET = """
local fill_rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

-- Load current state; a missing/expired key is a brand-new, full bucket.
local state = redis.call('HMGET', KEYS[1], 'tokens', 'last_refill')
local tokens
local last_refill
if state[1] == false then
    tokens = capacity
    last_refill = now
else
    tokens = tonumber(state[1])
    last_refill = tonumber(state[2])
    if last_refill == nil then
        last_refill = now
    end
end

-- Lazily refill based on elapsed wall-clock time (never go backwards).
local elapsed = now - last_refill
if elapsed < 0 then
    elapsed = 0
end
tokens = math.min(capacity, tokens + (elapsed * fill_rate))

-- Atomically check and consume.
local allowed = 0
local wait_ms = 0
if tokens >= requested then
    tokens = tokens - requested
    allowed = 1
else
    -- Milliseconds until the missing tokens refill; ceil so we never
    -- under-wait and busy-loop.
    wait_ms = math.ceil(((requested - tokens) / fill_rate) * 1000)
end

-- Persist the new state with full precision (avoid Lua number coercion).
redis.call('HSET', KEYS[1], 'tokens', string.format('%.6f', tokens),
           'last_refill', string.format('%.6f', now))

-- Expire idle keys automatically: a full refill window plus a 1s buffer.
local ttl_ms = math.ceil((capacity / fill_rate) * 1000) + 1000
if ttl_ms < 1000 then
    ttl_ms = 1000
end
redis.call('PEXPIRE', KEYS[1], ttl_ms)

return {allowed, wait_ms}
"""


# Redis exceptions that mean "Redis is unavailable" and must trigger graceful
# fallback to local limiting.  Computed once and guarded so the module never
# references ``redis.exceptions`` when redis-py is not installed (``redis is
# None``).  ``RedisError`` is the base of the redis-py exception hierarchy and
# therefore also covers response/script errors; ``ConnectionError`` (the
# explicit "Redis is DOWN" case) and ``TimeoutError`` are subclasses but are
# listed explicitly for clarity.
if redis is not None:
    _REDIS_ERRORS = (
        redis.exceptions.ConnectionError,
        redis.exceptions.TimeoutError,
        redis.exceptions.RedisError,
    )
else:  # pragma: no cover - exercised only when redis-py is absent
    _REDIS_ERRORS = ()


class GlobalTokenBucket(TokenBucket):
    """Token bucket whose state is shared across workers via Redis.

    Drop-in replacement for :class:`kombu.utils.limits.TokenBucket`: only
    :meth:`can_consume` and :meth:`expected_time` are overridden to consult a
    shared Redis key through an atomic Lua script.  ``add``, ``pop``,
    ``contents``, ``clear_pending`` and ``capacity`` are inherited unchanged so
    the bucket behaves identically everywhere the worker uses it (enforcement,
    scheduling, reset and shutdown).

    When Redis is unavailable -- redis-py not installed, a missing/invalid URL,
    or the server being down -- every Redis interaction degrades to the
    inherited local token-bucket math, so the worker keeps running (limited
    per-worker) instead of raising.  That degradation is also *bounded in
    latency*: the client is built with a default ``socket_connect_timeout`` and
    ``socket_timeout`` (:data:`DEFAULT_SOCKET_TIMEOUT`) so an unreachable Redis
    fails fast rather than stalling the admission hot path on the OS TCP
    timeout.  Operators can override the bound via the Redis URL query string
    (for example ``redis://host:6379/0?socket_connect_timeout=5``).

    Arguments:
        fill_rate (float): tokens added per second (the parsed ``rate_limit``).
        capacity (float): maximum tokens held; this feature always uses ``1``.
        redis_url (str): URL used to build the shared Redis client.
        key (str): per-task Redis key, namespaced per task name so distinct
            tasks keep independent global buckets while all workers running the
            same task contend on one shared key.
        client (redis.Redis): optional pre-built client (used by tests to point
            the bucket at a specific Redis instance); takes precedence over
            ``redis_url``.
    """

    def __init__(self, fill_rate, capacity=1, *, redis_url, key, client=None):
        # Initialise the base bucket first: this sets ``fill_rate``,
        # ``capacity``, ``_tokens``, ``timestamp`` and the ``contents`` deque,
        # which power both the inherited pending-request API and the local
        # fallback math used when Redis is unavailable.
        super().__init__(fill_rate, capacity)
        self.redis_url = redis_url
        # Per-task Redis key; isolation between tasks is achieved purely by
        # using a distinct key per task name (no other per-task state needed).
        self.key = key
        # Cached wait (seconds) from the most recent ``can_consume`` so
        # ``expected_time`` can answer without a second Redis round-trip.
        self._wait = 0.0
        # Whether the most recent ``can_consume`` decision came from the local
        # fallback path (Redis absent/unconfigured, or a Redis error raised at
        # call time).  When True, ``expected_time`` must use the inherited local
        # token-bucket math instead of the cached Redis ``_wait`` -- otherwise a
        # Redis outage would surface a stale wait (for example ``0.0``) and
        # busy-loop the worker's reschedule timer.  Initialised True so that a
        # never-consumed bucket reports inherited local timing until a real
        # Redis decision is recorded.
        self._used_local_fallback = True
        # Build the client once.  An injected client wins (testability);
        # otherwise build it from the URL when redis-py is installed.  Any
        # failure here (for example a malformed URL) degrades to local limiting
        # -- the bucket must never raise during construction.
        if client is not None:
            self._client = client
        elif redis is not None and redis_url:
            # Narrow the construction guard to the *expected* setup failures so
            # a genuine programming error is never silently swallowed: a
            # malformed URL raises ``ValueError`` and a misconfigured/unreachable
            # server raises a redis-py error (``_REDIS_ERRORS``).  Both degrade
            # to local limiting; any other exception is a real defect and is
            # allowed to propagate.
            try:
                # Inject a small default socket timeout so a Redis outage
                # degrades the admission hot path *quickly* instead of hanging
                # on the OS TCP connect timeout (~127s) before the can_consume
                # fallback fires.  Operator-supplied URL query params (for
                # example ``?socket_connect_timeout=5``) still override these
                # defaults, so no new user-facing setting is added.
                self._client = redis.from_url(
                    redis_url,
                    socket_connect_timeout=DEFAULT_SOCKET_TIMEOUT,
                    socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                )
            except (ValueError, *_REDIS_ERRORS):
                self._client = None
        else:
            self._client = None
        # ``register_script`` returns a callable that runs the script via
        # EVALSHA and transparently falls back to EVAL if the script is
        # evicted, so no bespoke script caching is required.
        if self._client is not None:
            # Only a Redis error here should degrade to local limiting; a
            # non-Redis error (for example an invalid injected client) is a real
            # defect and must surface rather than be hidden.  ``register_script``
            # merely computes the script's SHA, so in practice this rarely
            # raises.
            try:
                self._script = self._client.register_script(LUA_TOKEN_BUCKET)
            except _REDIS_ERRORS:
                self._script = None
        else:
            self._script = None

    def can_consume(self, tokens=1):
        """Consume ``tokens`` from the shared bucket atomically via Redis.

        Runs the Lua token-bucket script on the shared per-task key so the
        refill/check/consume happens in one atomic step (race-free across
        workers).  Caches the returned wait for :meth:`expected_time` and
        returns whether the tokens were granted.  On any Redis problem -- or
        when no client/script is configured -- it delegates to the inherited
        local token-bucket behaviour so the worker keeps functioning.
        """
        if self._script is None:
            # No Redis configured/available: use the local per-worker bucket and
            # mark the decision local so ``expected_time`` uses local timing.
            self._used_local_fallback = True
            return super().can_consume(tokens)
        try:
            allowed, wait_ms = self._script(
                keys=[self.key],
                args=[self.fill_rate, self.capacity, tokens, time.time()],
            )
            # Lua returns integer milliseconds; recover fractional seconds.
            self._wait = float(wait_ms) / 1000.0
            # Redis answered: ``expected_time`` may use the cached wait.
            self._used_local_fallback = False
            return bool(allowed)
        except _REDIS_ERRORS:
            # Redis is DOWN: degrade to local limiting, never propagate.  Mark
            # the decision local so the paired ``expected_time`` call returns the
            # inherited local wait (not the stale cached Redis ``_wait``), which
            # prevents an immediate-reschedule busy-loop during the outage.
            self._used_local_fallback = True
            return super().can_consume(tokens)

    def expected_time(self, tokens=1):
        """Return seconds to wait before ``tokens`` can be consumed.

        In the worker's scheduling loop this is called immediately after a
        failing :meth:`can_consume`, so when Redis answered that call it simply
        returns the wait cached then and avoids a second Redis round-trip.  It
        falls back to the inherited local estimate when no Redis client is
        configured *or* when the preceding :meth:`can_consume` degraded to the
        local bucket because Redis was down -- keeping the consume decision and
        the reported wait consistent and preventing a stale (for example
        ``0.0``) wait from busy-looping the worker's reschedule timer.
        """
        if self._script is None or self._used_local_fallback:
            return super().expected_time(tokens)
        return self._wait
