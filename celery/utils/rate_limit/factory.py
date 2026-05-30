"""Factory selecting the rate limiter (global Redis-backed vs per-process) for a task."""
from kombu.utils.limits import TokenBucket

from celery.utils.log import get_logger
from celery.utils.serialization import strtobool
from celery.utils.time import rate

from .global_limiter import GlobalRateLimiter

logger = get_logger(__name__)


def get_rate_limiter_for_task(app, task):
    """Return the rate-limiter bucket for ``task`` (or ``None``).

    This is the single decision point that the worker's
    ``Consumer.bucket_for_task`` delegates to (it calls
    ``get_rate_limiter_for_task(self.app, type)``, so ``task`` is the task
    *type* object exposing ``.name`` and ``.rate_limit``).

    The selection matrix mirrors -- and for the non-Redis branches reproduces
    bit-for-bit -- the legacy behaviour of ``bucket_for_task``
    (``celery/worker/consumer/consumer.py``):

    * ``rate_limit`` parses to ``0``/``None`` -> ``None`` (task is unthrottled).
    * the global feature is opted out
      (``worker_global_rate_limit_enabled = False``) -> a per-process
      ``kombu.utils.limits.TokenBucket`` (the exact legacy object).
    * no Redis broker/backend is reachable -> the same per-process
      ``TokenBucket``.
    * Redis is reachable and the feature is enabled -> a
      :class:`~celery.utils.rate_limit.global_limiter.GlobalRateLimiter`
      that shares its token state cluster-wide via an atomic Redis Lua script.

    Every branch returns either ``None`` or an object conforming to the
    ``TokenBucket`` protocol, so the consumer's downstream scheduling loop and
    the strategy integration require no modification.
    """
    # NOTE: global rate limiter entry point. Selects a cluster-wide Redis-backed
    # GlobalRateLimiter when Redis is reachable and the feature is enabled,
    # otherwise falls back to the legacy per-process TokenBucket (or None).
    limit = rate(getattr(task, 'rate_limit', None))
    if not limit:
        # Preserves legacy semantics: bucket_for_task returns None when the task
        # carries no (or a zero) rate_limit.  See celery/worker/consumer/consumer.py.
        return None
    # The opt-out flag is a bool by default, but when it is supplied through an
    # environment-variable-backed config source -- the standard
    # ``config_from_object(..., namespace='CELERY')`` pattern, e.g. the
    # ``CELERY_WORKER_GLOBAL_RATE_LIMIT_ENABLED`` environment variable -- it
    # arrives as a string such as 'False'/'0'/'no', all of which are truthy
    # under a plain ``bool()`` check and would silently defeat the opt-out
    # (QA F4). Coerce bool-like strings with ``strtobool`` -- the same coercion
    # the ``Option(type='bool')`` system uses (see celery/app/defaults.py).
    # Real bool/None values pass through untouched; an unrecognised string keeps
    # its legacy truthiness so this never raises into the task-dispatch path.
    enabled = app.conf.worker_global_rate_limit_enabled
    if isinstance(enabled, str):
        try:
            enabled = strtobool(enabled)
        except TypeError:
            pass
    if not enabled:
        # Opt-out flag set -> exact legacy per-process behaviour.
        return TokenBucket(limit, capacity=1)
    redis_client = _discover_redis_client(app)
    if redis_client is None:
        # No Redis broker/backend reachable -> legacy per-process behaviour.
        return TokenBucket(limit, capacity=1)
    # R5 / §0.7.6 failure isolation: constructing GlobalRateLimiter registers the
    # Lua script on the Redis client, which can raise on an incompatible/bad
    # client. Catch ANY construction failure here so bucket_for_task never
    # propagates an exception -- degrade to the legacy per-process TokenBucket.
    try:
        return GlobalRateLimiter(limit, redis_client, task.name, capacity=max(1, int(limit)))
    except Exception as exc:
        # SECURITY (§0.7.6): log ONLY the exception class -- never the client,
        # backend, or broker/Redis URL, which may carry credentials.
        logger.warning(
            'Global rate limiter: construction failed (%s); '
            'using per-process rate limits.', exc.__class__.__name__)
        return TokenBucket(limit, capacity=1)


def _discover_redis_client(app):
    """Return a reusable ``redis.StrictRedis`` client for ``app`` or ``None``.

    The client is discovered from existing Celery surfaces -- no second Redis
    dependency is introduced (AAP I5):

    * if the result backend is a ``RedisBackend``, its cached, pooled
      ``client`` is reused read-only;
    * otherwise, if the broker URL is ``redis://`` / ``rediss://``, the
      underlying client is taken from the broker connection's default channel.

    Discovery must never break task dispatch: any failure (including a missing
    optional ``redis`` extra, which makes importing ``RedisBackend`` raise
    ``ModuleNotFoundError``) is caught, logged at WARNING, and degrades to
    ``None`` so the caller falls back to the in-memory per-process bucket.
    """
    try:
        # Deferred import: avoids a load-time import cycle with the backend layer,
        # and avoids ModuleNotFoundError when the optional `redis` extra is absent
        # (celery/backends/redis.py performs an unguarded `from redis import ...`).
        from celery.backends.redis import RedisBackend
        if isinstance(app.backend, RedisBackend):
            # Reuse the cached, pooled redis.StrictRedis from the result backend.
            return app.backend.client
        broker_url = app.conf.broker_url
        if broker_url and (broker_url.startswith('redis://') or
                           broker_url.startswith('rediss://')):
            connection = app.connection_for_read()
            return connection.default_channel.client
        return None
    except Exception as exc:
        # Discovery must never break task dispatch; degrade to per-process limits.
        # SECURITY (§0.7.6): log ONLY the exception class -- never the broker/Redis
        # URL, which may carry credentials.
        logger.warning(
            'Global rate limiter: Redis client discovery failed (%s); '
            'using per-process rate limits.', exc.__class__.__name__)
        return None
