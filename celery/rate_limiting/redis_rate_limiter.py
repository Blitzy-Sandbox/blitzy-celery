"""Redis-backed global (cluster-wide) rate limiter using an atomic Lua token-bucket script.

This module provides :class:`RedisRateLimiter`, the concrete Redis backend for
Celery's optional, opt-in *global* (cluster-wide) rate limiter.  Where Celery's
built-in per-process rate limiter applies a task's ``rate_limit`` independently
inside every worker process -- allowing an aggregate rate of up to ``N`` times
the configured value across ``N`` processes -- this limiter coordinates token
consumption through a *shared* Redis store so the configured rate becomes a true
aggregate ceiling for the whole cluster.

The check-and-consume decision is performed by a single atomic Redis ``EVAL`` of
the :data:`TOKEN_BUCKET_LUA` script (one network round-trip), so concurrent
workers can never race each other: there is no Time-Of-Check-To-Time-Of-Use
window between reading the bucket and consuming a token.

The feature is **opt-in, default-off, and must never crash the worker**.  The
Redis client (``redis-py``) is *not* a hard dependency of Celery -- it is
provided optionally through the ``kombu[redis]`` extra -- so it is imported
lazily under a guard that mirrors :mod:`celery.backends.redis`.  When the client
library is missing, the configured backend URL is unreachable, or the Lua
evaluation fails for any reason, :meth:`RedisRateLimiter.can_consume` logs a
single sanitized warning and raises
:exc:`~celery.rate_limiting.base.RateLimiterUnavailable`, signalling the caller
to fall back to Celery's existing per-process behaviour.  Raising (rather than
returning ``(True, 0.0)``) is what keeps a backend outage distinguishable from a
genuine cluster-wide grant, so the worker preserves per-process limiting instead
of silently dispatching unlimited tasks while Redis is down.

Reused, never re-implemented:

* :func:`celery.utils.time.rate` parses ``"10/s"`` / ``"100/m"`` / ``"1000/h"``
  rate strings into tokens-per-second.
* :func:`kombu.utils.url.maybe_sanitize_url` masks any credentials embedded in
  the backend URL before it is stored or logged.
* :func:`celery.utils.log.get_logger` supplies the module logger.
"""
from __future__ import annotations

import time

from kombu.utils.url import maybe_sanitize_url

from celery.rate_limiting.base import BaseRateLimiter, RateLimiterUnavailable
from celery.utils.log import get_logger
from celery.utils.time import rate as rate_to_tps  # parses "10/s"/"100/m"/"1000/h" -> tokens/sec

try:
    import redis
except ImportError:
    redis = None  # graceful fallback when redis-py is absent

logger = get_logger(__name__)

__all__ = ('RedisRateLimiter',)

#: Prefix for every per-task bucket key written to Redis.  Keys are namespaced
#: under ``celery:global_rate_limit:`` so they never collide with Celery's
#: result-backend or broker keys, and are trivial to inspect or flush.
KEY_PREFIX = 'celery:global_rate_limit:'

#: Default Redis socket timeouts (in seconds) for the limiter's connection pool.
#:
#: The limiter sits on the worker's hot task-dispatch path, so it must *fail
#: open promptly* when Redis is unreachable rather than block for the OS-level
#: TCP timeout (which can be tens of seconds).  These deliberately short
#: defaults bound how long a blackholed/unreachable endpoint can stall dispatch
#: before :meth:`RedisRateLimiter.can_consume` catches the connection/timeout
#: error and falls back to the per-process limiter.  They are tunable per
#: instance via the constructor and are overridden by any
#: ``socket_connect_timeout`` / ``socket_timeout`` query parameter embedded in
#: the backend URL (redis-py resolves URL querystring args ahead of kwargs).
DEFAULT_SOCKET_CONNECT_TIMEOUT = 0.2
DEFAULT_SOCKET_TIMEOUT = 0.2

#: Atomic token-bucket refill-and-consume implemented as a single Lua script.
#:
#: Evaluating the whole read-modify-write inside Redis (one ``EVAL``) makes the
#: check-and-consume indivisible: two workers calling concurrently are
#: serialised by Redis, eliminating the cross-worker race that a multi-round
#: ``GET``/``SET`` sequence would suffer from.
#:
#: Contract::
#:
#:     KEYS[1] = per-task bucket key (a hash with fields ``tokens`` & ``timestamp``)
#:     ARGV[1] = rate       (tokens per second, float as string)
#:     ARGV[2] = capacity   (max tokens / burst size, float as string)
#:     ARGV[3] = now        (current wall-clock time in seconds, float as string)
#:     ARGV[4] = requested  (tokens to consume; the worker requests 1 per task)
#:
#: Returns a two-element array ``{allowed, retry_after}`` where ``allowed`` is
#: the integer ``1`` (token consumed, may run now) or ``0`` (denied), and
#: ``retry_after`` is the number of seconds to wait before retrying, encoded as
#: a *string*.  The string encoding is deliberate: Redis truncates Lua numbers
#: to integers on the way out, which would discard the fractional part of
#: ``retry_after``; returning ``tostring(retry_after)`` preserves it so Python
#: can parse it back to a ``float``.
TOKEN_BUCKET_LUA = """
-- Atomic token-bucket: refill based on elapsed time, then consume-or-deny.
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

-- 1. Load the current bucket state; initialise to a full bucket if absent.
local data = redis.call('HMGET', KEYS[1], 'tokens', 'timestamp')
local tokens = tonumber(data[1])
local last = tonumber(data[2])
if tokens == nil then
    tokens = capacity
end
if last == nil then
    last = now
end

-- 2. Refill: add the tokens accrued since the last update, capped at capacity.
local elapsed = now - last
if elapsed < 0 then
    elapsed = 0
end
tokens = tokens + (elapsed * rate)
if tokens > capacity then
    tokens = capacity
end

-- 3. Consume if enough tokens are available, otherwise compute the wait time.
local allowed = 0
local retry_after = 0
if tokens >= requested then
    tokens = tokens - requested
    allowed = 1
    retry_after = 0
else
    allowed = 0
    if rate > 0 then
        retry_after = (requested - tokens) / rate
    else
        retry_after = 0
    end
end

-- 4. Persist the new state.
redis.call('HSET', KEYS[1], 'tokens', tokens, 'timestamp', now)

-- 5. Expire the key so windows reset and a crashed worker never holds tokens
--    forever.  One full refill period plus a second of slack is ample.
local ttl_ms
if rate > 0 then
    ttl_ms = math.ceil((capacity / rate) + 1) * 1000
else
    ttl_ms = 1000
end
redis.call('PEXPIRE', KEYS[1], ttl_ms)

-- 6. Return allowed (int) and retry_after (string, to preserve the fraction).
return {allowed, tostring(retry_after)}
"""


class RedisRateLimiter(BaseRateLimiter):
    """Cluster-wide rate limiter backed by an atomic Redis token bucket.

    Each task name is mapped to its own Redis hash (a *bucket*) keyed as
    ``celery:global_rate_limit:{task_name}``.  Every :meth:`can_consume` call
    evaluates :data:`TOKEN_BUCKET_LUA` once: the script refills the bucket
    according to the time elapsed since the previous call, consumes a token if
    one is available, sets a TTL so the window resets cleanly, and returns the
    decision.  Because the entire refill-and-consume runs inside a single Redis
    ``EVAL``, the decision is atomic across every worker process in the cluster
    and the configured ``rate_limit`` becomes a true aggregate ceiling.

    The limiter is intentionally **fail-safe**: it never lets a backend problem
    crash the worker.  If ``redis-py`` is not installed, the backend URL cannot
    be reached, or the Lua evaluation errors, the limiter logs exactly one
    sanitized warning per instance and raises
    :exc:`~celery.rate_limiting.base.RateLimiterUnavailable` so the worker falls
    back to Celery's existing per-process rate limiting.  Raising a dedicated
    exception -- rather than returning ``(True, 0.0)`` as a real grant would --
    is what lets :mod:`celery.worker.strategy` tell a backend outage apart from
    a genuine cluster-wide grant and keep the per-process bucket intact on
    fallback.

    Arguments:
        backend_url (str): The Redis connection URL the limiter coordinates
            through (for example ``"redis://localhost:6379/0"``).  It may embed
            credentials; only a sanitized copy is ever logged.
        capacity (float, optional): Alias for ``default_capacity`` accepted for
            API convenience.  ``default_capacity`` takes precedence when both
            are supplied.
        default_capacity (float, optional): Fixed bucket capacity (maximum
            burst, in tokens) to use for every task.  When ``None`` (the
            default) the capacity is derived per task from its rate at call
            time -- see :meth:`can_consume`.
        socket_connect_timeout (float, optional): Seconds to wait when opening
            the TCP connection to Redis before failing open.  Defaults to
            :data:`DEFAULT_SOCKET_CONNECT_TIMEOUT` so an unreachable endpoint
            never stalls task dispatch for the OS-level TCP timeout.  A
            ``socket_connect_timeout`` query parameter in ``backend_url``
            overrides this value.
        socket_timeout (float, optional): Seconds to wait for a Redis reply
            (the Lua ``EVAL``/``EVALSHA`` round-trip) before failing open.
            Defaults to :data:`DEFAULT_SOCKET_TIMEOUT`.  A ``socket_timeout``
            query parameter in ``backend_url`` overrides this value.
    """

    def __init__(self, backend_url, capacity=None, default_capacity=None,
                 socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
                 socket_timeout=DEFAULT_SOCKET_TIMEOUT):
        # Keep the raw URL for building the client, but only ever log/store a
        # sanitized copy so embedded credentials are never written to logs.
        self._url = backend_url
        self._sanitized_url = (
            maybe_sanitize_url(backend_url) if backend_url else backend_url
        )
        # ``default_capacity`` is the documented name; ``capacity`` is accepted
        # as an alias.  ``default_capacity`` wins when both are provided; if
        # neither is given the capacity is derived per-rate inside can_consume.
        self._default_capacity = (
            default_capacity if default_capacity is not None else capacity
        )
        # Sane, short socket timeouts so an unreachable Redis fails open
        # promptly instead of blocking dispatch for the OS-level TCP timeout.
        # Passed to ConnectionPool.from_url; any timeout embedded in the URL
        # query string takes precedence (redis-py resolves URL args first).
        self._socket_connect_timeout = socket_connect_timeout
        self._socket_timeout = socket_timeout
        # Lazily-built client/script and a one-shot guard so the fallback
        # warning is logged at most once per limiter instance (no log flooding
        # under high task volume).  No connection is opened in __init__ so
        # construction can never fail or block.
        self._client = None
        self._script = None
        self._fallback_warned = False

    def _get_client(self):
        """Return a connected Redis client, or ``None`` to signal fallback.

        The connection pool, client, and registered Lua script are built once
        on first use and cached for the limiter's lifetime, mirroring the
        connection-pool reuse of :mod:`celery.backends.redis`.  Unlike the
        result backend -- which raises :exc:`ImproperlyConfigured` when the
        client library is missing -- this helper degrades gracefully: a missing
        ``redis-py`` library or any error while building the pool/client
        results in ``None`` rather than an exception, so the worker is never
        crashed by an unavailable limiter.

        Returns:
            redis.Redis | None: The cached client, or ``None`` when the client
            library is absent or construction failed.
        """
        # redis-py is optional (supplied via the kombu[redis] extra); when it
        # is not installed the guarded import above left ``redis`` as None.
        if redis is None:
            return None
        try:
            if self._client is None:
                # ConnectionPool.from_url does not open a socket, so building
                # it here is cheap; pooling amortises connection setup across
                # the high volume of can_consume calls.  The socket timeouts
                # bound every connect and reply (including the Lua EVAL/EVALSHA
                # round-trip) so an unreachable Redis fails open promptly rather
                # than blocking dispatch; a timeout supplied in the URL query
                # string overrides these defaults (redis-py: URL args win).
                pool = redis.ConnectionPool.from_url(
                    self._url,
                    socket_connect_timeout=self._socket_connect_timeout,
                    socket_timeout=self._socket_timeout,
                )
                self._client = redis.Redis(connection_pool=pool)
                # Register the script once; subsequent calls reuse the cached
                # SHA via EVALSHA, keeping each consume to a single round-trip.
                self._script = self._client.register_script(TOKEN_BUCKET_LUA)
            return self._client
        except Exception:
            # Never let client setup crash the worker.
            # A malformed URL or any other construction error must degrade to
            # the per-process fallback, not propagate to the dispatch path.
            return None

    def _warn_fallback(self, exc=None):
        """Log a single sanitized fallback warning for this limiter instance.

        The warning is emitted at most once per instance (guarded by
        ``self._fallback_warned``) to avoid flooding the logs when Redis is
        unavailable under high task volume.  Only the *sanitized* backend URL
        is logged so credentials embedded in the URL are never disclosed.

        Arguments:
            exc (Exception, optional): The originating exception, if any, logged
                with ``%r`` for diagnostic context.
        """
        if self._fallback_warned:
            return
        self._fallback_warned = True
        logger.warning(
            'Global rate limiter unavailable (redis backend %s); '
            'falling back to per-process rate limiting: %r',
            self._sanitized_url, exc,
        )

    def can_consume(self, task_name, rate):
        """Atomically decide whether ``task_name`` may be dispatched now.

        Arguments:
            task_name (str): The fully-qualified task name, used to key the
                per-task bucket (``celery:global_rate_limit:{task_name}``).
            rate: The task's declared ``rate_limit`` -- a rate string such as
                ``"10/s"``, ``"100/m"`` or ``"1000/h"``, or a number of tasks
                per second.  An unset or zero rate is a no-op.

        Returns:
            tuple[bool, float]: ``(allowed, retry_after)``.  ``allowed`` is
            ``True`` when a token was consumed and the task may run now (with
            ``retry_after`` ``0.0``); otherwise ``allowed`` is ``False`` and
            ``retry_after`` is the number of seconds to wait before re-queueing.
            An unset or zero rate is a no-op and returns ``(True, 0.0)`` without
            touching Redis (it does **not** raise).

        Raises:
            RateLimiterUnavailable: When the backend cannot enforce the limit --
                ``redis-py`` is missing, the pool/client cannot be built, the
                rate string is malformed, or the Lua ``EVAL`` errors.  The
                limiter logs a single sanitized warning per instance first, then
                raises so the caller falls back to per-process rate limiting.
                Raising (instead of returning ``(True, 0.0)``) keeps a backend
                outage distinguishable from a genuine grant; see
                :class:`~celery.rate_limiting.base.RateLimiterUnavailable`.  No
                other exception ever escapes this method.
        """
        # Reuse Celery's existing parser; it returns tokens-per-second and
        # yields 0 for None/empty/zero rates (and accepts plain numbers too).
        # A malformed rate string (e.g. 'bad' -> ValueError, '10/x' -> KeyError)
        # makes the parser raise; honour the documented fallback contract by
        # warning once and signalling fallback (raise RateLimiterUnavailable)
        # rather than letting the parse error escape into the dispatch path.
        try:
            tps = rate_to_tps(rate)
        except Exception as exc:
            # The rate string is malformed, so the limiter cannot enforce a
            # cluster-wide ceiling: warn once and signal fallback to the
            # per-process path rather than let the parse error escape into the
            # worker's dispatch path.  Raising (vs returning a grant) keeps this
            # distinct from a real grant so the per-process bucket is preserved.
            self._warn_fallback(exc)
            raise RateLimiterUnavailable(
                f'invalid rate {rate!r} for task {task_name!r}') from exc
        if not tps:
            # An unset or zero rate means the global limiter is not consulted;
            # do not touch Redis at all -- this is a pure no-op.
            return (True, 0.0)

        client = self._get_client()
        if client is None:
            # redis-py missing or the pool/client failed to build: warn once
            # and signal fallback so the caller uses the existing per-process
            # limiter.  Raising (vs returning a grant) keeps this distinct from
            # a real Redis-enforced grant so the per-process bucket is preserved.
            self._warn_fallback()
            raise RateLimiterUnavailable(
                f'redis client unavailable for task {task_name!r}')

        # Capacity = one second's worth of tokens preserves the aggregate
        # ceiling, while max(1.0, tps) guarantees a single task is never
        # permanently blocked for slow rates such as "1000/h" (tps < 1).
        capacity = (
            self._default_capacity if self._default_capacity else max(1.0, tps)
        )
        key = f'{KEY_PREFIX}{task_name}'
        now = time.time()
        requested = 1
        try:
            # Single atomic EVAL (one round-trip) -- no separate GET/SET, so
            # concurrent workers cannot race on the same bucket.
            result = self._script(
                keys=[key], args=[tps, capacity, now, requested],
            )
            allowed_raw, retry_raw = result[0], result[1]
            allowed = bool(int(allowed_raw))
            # retry_after is returned as a string to survive Redis' integer
            # coercion of Lua numbers; it may arrive as bytes or str depending
            # on the client's decode settings.
            if isinstance(retry_raw, bytes):
                retry_raw = retry_raw.decode('utf-8')
            retry_after = float(retry_raw)
            return (allowed, retry_after if not allowed else 0.0)
        except Exception as exc:
            # Never crash the worker on a backend problem.
            # Covers redis.exceptions.RedisError, ConnectionError, timeouts and
            # any Lua/runtime error: warn once and signal fallback so the caller
            # degrades to the per-process limiter.  Raising (vs returning a
            # grant) keeps this distinct from a real grant so the per-process
            # bucket is preserved during a Redis outage.
            self._warn_fallback(exc)
            raise RateLimiterUnavailable(
                f'redis evaluation failed for task {task_name!r}') from exc
