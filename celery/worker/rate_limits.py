"""Redis-backed global (cluster-wide) task rate limiting.

This module implements the opt-in global rate limiter activated by the
:setting:`worker_rate_limits_global` setting.  When enabled, a task's
existing ``rate_limit`` (for example ``"10/s"``) becomes the *aggregate*
ceiling across **all** workers instead of being enforced independently by
each worker process.

The token-bucket state for every task lives in a single Redis hash and the
entire refill-check-consume cycle runs inside one atomic server-side Lua
script.  Because the decision is made on the Redis server, concurrent
workers cannot double-spend tokens, and refill timing uses the Redis server
clock so worker clock skew is irrelevant.

The module is imported lazily by
:meth:`~celery.worker.consumer.consumer.Consumer.bucket_for_task` *only* when
the feature is enabled, so the default (disabled) path stays import-free and
connection-free.
"""
from time import monotonic
from weakref import WeakKeyDictionary

from kombu.utils.limits import TokenBucket

from celery.exceptions import ImproperlyConfigured
from celery.utils.log import get_logger

try:
    import redis
except ImportError:
    redis = None

__all__ = ('RedisTokenBucket', 'get_limiter_client', 'rate_limit_key')

logger = get_logger(__name__)

E_REDIS_MISSING = """
You need to install the redis library in order to use \
the Redis-backed global (cluster-wide) task rate limiter \
(worker_rate_limits_global).
"""

E_NO_REDIS_URL = """
The global (cluster-wide) task rate limiter (worker_rate_limits_global) \
requires a coordinating Redis server.  Set worker_rate_limit_url to a \
redis://, rediss:// or redis+socket:// URL, or configure a Redis \
result_backend or broker_url.
"""

#: URL schemes accepted for the coordinating Redis server.  TLS is supported
#: through ``rediss://`` and unix sockets through ``redis+socket://``.
REDIS_SCHEMES = ('redis://', 'rediss://', 'redis+socket://')

#: Short socket read *and* connect timeout (seconds).  Deliberately small so a
#: slow or unreachable Redis can never stall the eventlet/gevent hub; on
#: timeout the limiter fails open to a local token bucket.
SOCKET_TIMEOUT = 2.0

#: Floor for the per-key TTL (seconds) so idle task buckets self-expire while
#: an active bucket is never evicted between two admissions.
MIN_BUCKET_TTL = 60

#: Minimum number of seconds between fail-open warnings emitted per bucket,
#: preventing repeated Redis errors from flooding the log.
WARN_THROTTLE = 60.0


# Atomic token-bucket admission script.  The whole refill-check-consume cycle
# runs server-side in a single round trip so concurrent workers cannot
# double-spend tokens.  "now" is derived from the Redis server clock (TIME)
# rather than a client timestamp, so worker clock skew is irrelevant, and the
# script touches a single key which keeps it Redis-Cluster safe.
#
#   KEYS[1] -- per-task bucket hash key
#   ARGV[1] -- fill_rate (tokens/second, float)
#   ARGV[2] -- capacity (maximum tokens, float)
#   ARGV[3] -- ttl (seconds, int)
#   ARGV[4] -- requested tokens (int)
#
# Returns ``{allowed, wait_ms}``: ``allowed`` is 1/0 and ``wait_ms`` is the
# time to wait before the request can be admitted (0 when allowed).  The
# ``math.ceil`` guards against Redis truncating a sub-millisecond positive
# wait down to 0, which would otherwise hot-loop the consumer timer.
TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local fill_rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local t = redis.call('TIME')
local now = tonumber(t[1]) + (tonumber(t[2]) / 1000000.0)

local data = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(data[1])
local last_refill = tonumber(data[2])

if tokens == nil or last_refill == nil then
    tokens = capacity
    last_refill = now
end

local elapsed = now - last_refill
if elapsed < 0 then elapsed = 0 end
tokens = math.min(capacity, tokens + (elapsed * fill_rate))
last_refill = now

local allowed = 0
local wait_ms = 0
if tokens >= requested then
    tokens = tokens - requested
    allowed = 1
else
    wait_ms = math.ceil(((requested - tokens) / fill_rate) * 1000.0)
end

redis.call('HSET', key, 'tokens', tokens, 'last_refill', last_refill)
redis.call('EXPIRE', key, ttl)
return {allowed, wait_ms}
"""


#: Redis key prefix for every per-task global rate-limit bucket hash.  The full
#: key built by :func:`rate_limit_key` is
#: ``celery:rate_limit:{app.main or 'celery'}:{task_name}``, so the app name
#: always namespaces the bucket.
KEY_PREFIX = 'celery:rate_limit'


def rate_limit_key(app, task_name):
    """Build the app-namespaced Redis key for a task's rate-limit bucket.

    This is the single, canonical key builder for the global rate limiter; the
    consumer integration must route every :class:`RedisTokenBucket` through it
    so the key scheme stays consistent across workers and so that two Celery
    apps which share one Redis -- and happen to register a task of the same
    name -- never collide on a single global bucket.

    The returned key is ``celery:rate_limit:{app.main or 'celery'}:{task_name}``.
    The app segment falls back to ``'celery'`` when :attr:`app.main` is unset,
    giving an unnamed app a stable namespace of its own rather than an empty one.

    Arguments:
        app (Celery): The Celery application owning the task; its
            :attr:`~celery.Celery.main` name namespaces the key.
        task_name (str): The registered task name (for example
            ``"tasks.send_sms"``).

    Returns:
        str: The fully-qualified Redis key for this task's bucket hash.
    """
    return f'{KEY_PREFIX}:{app.main or "celery"}:{task_name}'


class RedisTokenBucket(TokenBucket):
    """Token bucket whose admission decision is coordinated through Redis.

    This subclass of :class:`kombu.utils.limits.TokenBucket` keeps the entire
    per-worker request queue (the inherited :attr:`contents` deque together
    with :meth:`add`, :meth:`pop` and :meth:`clear_pending`) **unchanged**, so
    it remains a drop-in replacement inside
    :meth:`~celery.worker.consumer.consumer.Consumer._schedule_bucket_request`.
    Only the *token decision* becomes global: :meth:`can_consume` and
    :meth:`expected_time` are overridden to consult a shared Redis bucket via
    an atomic Lua script instead of the local in-memory counter.

    When Redis cannot be reached during a decision the bucket *fails open*: it
    logs a throttled warning and delegates to a lazily created local
    :class:`~kombu.utils.limits.TokenBucket`, degrading gracefully to the
    per-worker behaviour rather than halting task consumption.

    Arguments:
        fill_rate (float): Tokens added per second (guaranteed > 0 by the
            caller, which only builds a bucket for a non-zero rate limit).
        capacity (float): Maximum number of tokens held by the bucket.
        client (redis.Redis): Redis client used to coordinate the bucket.
        key (str): Fully-qualified Redis key for this task's bucket hash, as
            built by :func:`rate_limit_key` -- the canonical, app-namespaced
            key builder the consumer integration must use so that two apps
            sharing one Redis never collide on a single bucket.
    """

    def __init__(self, fill_rate, capacity=1, *, client=None, key=None):
        super().__init__(fill_rate, capacity)
        self.client = client
        self.key = key
        # Register the Lua script once.  redis-py returns a Script object that
        # loads the body into the server script cache and transparently
        # reloads it on a NOSCRIPT error, so no manual EVALSHA/SCRIPT LOAD
        # bookkeeping is required.
        self._script = client.register_script(TOKEN_BUCKET_LUA)
        # Generous per-key TTL: at least ``MIN_BUCKET_TTL`` seconds, otherwise
        # twice the time needed to refill the whole bucket so slow rates are
        # not evicted between admissions (``fill_rate`` is > 0 by contract).
        self._ttl = max(MIN_BUCKET_TTL, int(self.capacity / self.fill_rate * 2))
        # Wait (seconds) cached by ``can_consume`` and returned unchanged by
        # ``expected_time`` so the latter performs no extra Redis round trip.
        self._last_wait = 0.0
        # Fail-open state: the local shadow bucket is built lazily and reused
        # so it keeps its per-worker token state across Redis outages.
        self._local_bucket = None
        self._last_warning = 0.0

    def can_consume(self, tokens=1):
        """Atomically attempt to consume ``tokens`` from the shared bucket.

        Runs the Lua script on Redis in a single round trip and caches the
        returned wait so :meth:`expected_time` needs no further call.  On a
        Redis error the bucket fails open to a local token bucket.

        Returns:
            bool: ``True`` if the tokens were consumed from the shared bucket.
        """
        try:
            allowed, wait_ms = self._script(
                keys=[self.key],
                args=[self.fill_rate, self.capacity, self._ttl, tokens],
            )
            # Lua returns integer milliseconds; convert to seconds for the
            # consumer's ``timer.call_after`` reschedule.
            self._last_wait = float(wait_ms) / 1000.0
            return bool(allowed)
        except redis.exceptions.RedisError as exc:
            # Fail open: degrade to per-worker limiting (an explicit
            # availability-over-strictness trade-off) instead of blocking
            # consumption when Redis is unreachable.
            self._warn_fail_open(exc)
            local = self._get_local_bucket()
            result = local.can_consume(tokens)
            self._last_wait = local.expected_time(tokens)
            return result

    def expected_time(self, tokens=1):
        """Return the wait (seconds) cached by the last :meth:`can_consume`.

        No extra Redis round trip is performed.  For sub-``1/s`` rates this is
        strictly positive (the Lua script computes the wait with
        ``math.ceil``), preventing a hot reschedule loop in the consumer.

        Returns:
            float: Seconds to wait before the request can be admitted.
        """
        return self._last_wait

    def _get_local_bucket(self):
        # Lazily build and cache the per-worker shadow bucket used while Redis
        # is unreachable, so it preserves token state across fail-open calls.
        if self._local_bucket is None:
            self._local_bucket = TokenBucket(self.fill_rate, self.capacity)
        return self._local_bucket

    def _warn_fail_open(self, exc):
        # Throttle warnings to at most one per ``WARN_THROTTLE`` seconds per
        # bucket so a persistent outage does not flood the log.
        now = monotonic()
        if now - self._last_warning >= WARN_THROTTLE:
            self._last_warning = now
            # SECURITY: reference only the task name (parsed from the key) and
            # the exception class name -- never the URL or any credentials.
            task_name = self.key.rsplit(':', 1)[-1] if self.key else '?'
            logger.warning(
                'Global rate limiter for task %r could not reach Redis (%s); '
                'falling back to per-worker rate limiting.',
                task_name, type(exc).__name__,
            )


#: Per-app cache of coordinating Redis clients.  A :class:`WeakKeyDictionary`
#: is used so exactly one client is built and reused per app without leaking a
#: strong reference to the app object.
_CLIENTS = WeakKeyDictionary()


def _is_redis_url(url):
    """Return :const:`True` if ``url`` is a string using an accepted scheme."""
    return isinstance(url, str) and url.startswith(REDIS_SCHEMES)


def get_limiter_client(app):
    """Return a cached Redis client coordinating the global rate limiter.

    The coordinating URL is resolved with the following precedence:

    #. :setting:`worker_rate_limit_url`, when set;
    #. :setting:`result_backend`, when it is a Redis URL;
    #. :setting:`broker_url`, when it is a Redis URL.

    Exactly one client is built per ``app`` -- with short socket timeouts so a
    slow Redis never stalls the eventlet/gevent hub -- and reused on every
    subsequent call.

    Arguments:
        app (Celery): The Celery application whose configuration is read.

    Returns:
        redis.Redis: The cached coordinating Redis client.

    Raises:
        ImproperlyConfigured: if the :pypi:`redis` library is not installed,
            or no Redis-scheme URL can be resolved from the configuration.
    """
    client = _CLIENTS.get(app)
    if client is not None:
        return client

    if redis is None:
        raise ImproperlyConfigured(E_REDIS_MISSING.strip())

    conf = app.conf
    url = conf.worker_rate_limit_url
    if not url:
        if _is_redis_url(conf.result_backend):
            url = conf.result_backend
        elif _is_redis_url(conf.broker_url):
            url = conf.broker_url

    if not _is_redis_url(url):
        # NOTE: never echo the offending URL -- it may embed credentials.
        raise ImproperlyConfigured(E_NO_REDIS_URL.strip())

    # redis-py's ``from_url`` understands redis://, rediss:// and unix://; map
    # the celery-style redis+socket:// scheme onto unix:// so socket URLs work.
    if url.startswith('redis+socket://'):
        url = 'unix://' + url[len('redis+socket://'):]

    client = redis.Redis.from_url(
        url,
        socket_timeout=SOCKET_TIMEOUT,
        socket_connect_timeout=SOCKET_TIMEOUT,
    )
    _CLIENTS[app] = client
    return client
