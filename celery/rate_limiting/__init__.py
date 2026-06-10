"""Global (cluster-wide) rate limiting for Celery tasks (opt-in, default-off).

This package implements an optional, **opt-in** global (cluster-wide) rate
limiter that coordinates a task's configured ``rate_limit`` across *every*
worker process in the cluster.  Celery's built-in rate limiter is enforced
independently inside each worker process, so a task configured at ``"10/s"``
can run at up to ``N * 10/s`` across ``N`` worker processes.  When this feature
is enabled, token consumption is coordinated through a shared store (Redis) so
the configured rate becomes a true aggregate ceiling regardless of how many
worker processes are running.

The feature is **default-off** and **never crashes the worker**:

* :class:`~celery.rate_limiting.base.BaseRateLimiter` -- the backend-agnostic
  contract exposing a single atomic ``can_consume`` decision.
* :class:`~celery.rate_limiting.redis_rate_limiter.RedisRateLimiter` -- the
  concrete Redis token-bucket backend (atomic Lua, graceful fallback).
* :func:`get_global_rate_limiter` -- the factory the worker dispatch strategy
  (:mod:`celery.worker.strategy`) calls; it returns a configured limiter only
  when the feature is enabled, otherwise ``None`` so wiring is a no-op.

Importing this package is safe on installations that do not have ``redis-py``
installed: the Redis client is imported lazily and under a guard inside
:mod:`celery.rate_limiting.redis_rate_limiter`, and a client is only ever
constructed from inside :func:`get_global_rate_limiter` when the feature is
explicitly enabled.
"""
from __future__ import annotations

from celery.rate_limiting.base import BaseRateLimiter
from celery.rate_limiting.redis_rate_limiter import RedisRateLimiter

__all__ = ('BaseRateLimiter', 'RedisRateLimiter', 'get_global_rate_limiter')


def get_global_rate_limiter(app):
    """Return a configured global rate limiter for ``app`` or ``None``.

    Reads the two settings registered in :mod:`celery.app.defaults`:
    ``global_rate_limit_enabled`` (bool, default ``False``) and
    ``global_rate_limit_backend_url`` (str, default ``None``).  Returns a
    :class:`~celery.rate_limiting.redis_rate_limiter.RedisRateLimiter` when
    global limiting is enabled *and* a backend URL is configured; otherwise
    returns ``None`` so wiring is a complete no-op in the default
    configuration (no Redis client is constructed).

    The factory is intentionally defensive and **never raises**: if reading the
    configuration or constructing the limiter fails for any reason, it returns
    ``None`` so the worker silently falls back to Celery's existing per-process
    rate limiting rather than failing to start.

    Arguments:
        app: The Celery application instance whose ``conf`` supplies the two
            global rate-limit settings.

    Returns:
        BaseRateLimiter | None: A configured
        :class:`~celery.rate_limiting.redis_rate_limiter.RedisRateLimiter`
        when the feature is enabled and a backend URL is present; ``None``
        otherwise (the default), signalling the caller to use the unchanged
        per-process rate limiter.
    """
    try:
        conf = app.conf
        # Read the new-style lowercase setting names; Celery's settings layer
        # resolves the legacy uppercase aliases (CELERY_GLOBAL_RATE_LIMIT_*) to
        # these.  Using ``.get(..., default)`` is the most defensive access and
        # never raises even if the keys were somehow not registered.
        enabled = conf.get('global_rate_limit_enabled', False)
        backend_url = conf.get('global_rate_limit_backend_url', None)
        # Engage only when explicitly enabled AND a backend URL is supplied; an
        # enabled flag with no URL stays a no-op (falls through to ``None``).
        if enabled and backend_url:
            return RedisRateLimiter(backend_url=backend_url)
    except Exception:
        # Defensive: never break worker startup because of limiter wiring. Any
        # failure (config access, client construction, ...) falls through to
        # the unchanged per-process rate-limiting path below.
        pass
    return None
