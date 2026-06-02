"""Opt-in, Redis-backed *global* rate limiting for Celery tasks.

This package isolates all logic for the global rate limiter. By default Celery
enforces a task's ``rate_limit`` *per worker process*; when the operator sets
``task_global_rate_limit_backend`` to a Redis URL the consumer's
``bucket_for_task()`` factory substitutes a :class:`RedisTokenBucket` so the
limit holds **globally across the entire worker fleet**.

:class:`RedisTokenBucket` is re-exported here so the consumer can use the
package-level import ``from celery.rate_limiting import RedisTokenBucket`` at
its module top. The implementation lives in
:mod:`celery.rate_limiting.redis_rate_limiter`.
"""
from .redis_rate_limiter import RedisTokenBucket

__all__ = ('RedisTokenBucket',)
