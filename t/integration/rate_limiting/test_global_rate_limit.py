"""Integration tests for the global (cluster-wide) rate limiter.

These tests drive multiple *simulated workers* -- multiple
:class:`~celery.rate_limiting.RedisRateLimiter` instances sharing ONE running
Redis server -- and assert that the **aggregate** throughput respects the
configured cluster-wide ceiling (approximately the configured rate, NOT
``N x rate`` for ``N`` workers).  This proves cross-worker coordination via the
atomic Redis Lua token bucket.

This module is the integration-test counterpart to ``t/unit/rate_limiting``.
It degrades gracefully: when ``redis-py`` is not installed or no Redis server
is reachable, the Redis-backed tests SKIP cleanly (they never ERROR), mirroring
the ``test_task_redis_result_backend`` pattern in ``t/integration/test_tasks.py``.
"""
import os
import time

import pytest

from celery import Celery
from celery.rate_limiting import RedisRateLimiter, get_global_rate_limiter

from ..conftest import flaky  # timeout=300 + reruns=5 (timing-sensitive wrapper)
from ..tasks import get_redis_connection  # honors REDIS_HOST / REDIS_PORT


def _redis_url():
    """Build a Redis URL from ``REDIS_HOST``/``REDIS_PORT`` (env-driven).

    Matches the shape of the user's documented example
    ``redis://localhost:6379/0`` and stays consistent with
    :func:`t.integration.tasks.get_redis_connection`.
    """
    host = os.environ.get('REDIS_HOST', 'localhost')
    port = os.environ.get('REDIS_PORT', 6379)
    return f'redis://{host}:{port}/0'


@pytest.fixture
def redis_backend_url():
    """Return a live Redis URL, or SKIP cleanly when Redis is unavailable.

    Mirrors ``t/integration/test_tasks.py::test_task_redis_result_backend``:
    skip (never error) when ``redis-py`` is missing or no server answers.
    """
    redis = pytest.importorskip('redis')  # skip if redis-py is not installed
    host = os.environ.get('REDIS_HOST', 'localhost')
    port = os.environ.get('REDIS_PORT', 6379)
    try:
        redis.StrictRedis(host=host, port=port).ping()
    except Exception:
        pytest.skip('Requires a running Redis server.')
    return _redis_url()


class test_global_rate_limit:
    """Aggregate, cluster-wide rate-limit behavior against a live Redis."""

    @flaky
    def test_aggregate_ceiling_across_simulated_workers(self, redis_backend_url):
        """N limiters sharing ONE Redis must not exceed the cluster ceiling.

        With ``"5/s"`` the shared bucket has capacity ``max(1.0, 5.0) == 5`` and
        refills 5 tokens/sec.  In a tight loop the refill over a few ms is
        negligible, so the number of grants is bounded by the capacity (~5),
        NOT by ``workers * attempts`` -- which proves cross-worker coordination.
        """
        task_name = 't.integration.rate_limiting.aggregate'
        rate = '5/s'
        # Clean the shared bucket key first so the test is deterministic.
        get_redis_connection().delete(f'celery:global_rate_limit:{task_name}')

        workers = 4
        attempts_each = 20
        limiters = [RedisRateLimiter(backend_url=redis_backend_url)
                    for _ in range(workers)]

        total_allowed = 0
        for _ in range(attempts_each):
            for limiter in limiters:
                allowed, _retry_after = limiter.can_consume(task_name, rate)
                if allowed:
                    total_allowed += 1

        total_attempts = workers * attempts_each  # 80
        # At least one grant (the bucket started full).
        assert total_allowed >= 1
        # Cluster-wide ceiling: capacity (5) plus a tiny refill margin.
        assert total_allowed <= 7
        # Proves coordination, not N x rate: far below the uncoordinated bound.
        assert total_allowed < total_attempts

    @flaky
    def test_window_resets_and_refills_over_time(self, redis_backend_url):
        """Draining the bucket denies further calls until the window refills."""
        task_name = 't.integration.rate_limiting.window_reset'
        rate = '5/s'
        get_redis_connection().delete(f'celery:global_rate_limit:{task_name}')

        limiter = RedisRateLimiter(backend_url=redis_backend_url)

        granted = 0
        for _ in range(20):
            allowed, _retry_after = limiter.can_consume(task_name, rate)
            if allowed:
                granted += 1
        # A full bucket grants at least one token before being drained.
        assert granted >= 1

        # Bucket now drained -> the next call is denied with a positive wait.
        allowed, retry_after = limiter.can_consume(task_name, rate)
        assert allowed is False
        assert retry_after > 0

        # After ~1s the window refills (TTL-based reset) -> allowed again.
        time.sleep(1.1)
        allowed, retry_after = limiter.can_consume(task_name, rate)
        assert allowed is True
        assert retry_after == 0.0

    def test_unset_or_zero_rate_is_noop(self, redis_backend_url):
        """``None``/``0``/``"0/s"`` are no-ops returning ``(True, 0.0)``."""
        limiter = RedisRateLimiter(backend_url=redis_backend_url)
        task_name = 't.integration.rate_limiting.noop'
        assert limiter.can_consume(task_name, None) == (True, 0.0)
        assert limiter.can_consume(task_name, 0) == (True, 0.0)
        assert limiter.can_consume(task_name, '0/s') == (True, 0.0)

    def test_factory_returns_limiter_when_enabled(self, redis_backend_url):
        """Factory returns a ``RedisRateLimiter`` when enabled and URL is set."""
        app = Celery('grl_factory_enabled_test')
        app.conf.global_rate_limit_enabled = True
        app.conf.global_rate_limit_backend_url = redis_backend_url
        assert isinstance(get_global_rate_limiter(app), RedisRateLimiter)

    def test_factory_returns_none_when_disabled(self):
        """Factory returns ``None`` in the default (disabled) configuration."""
        app = Celery('grl_factory_disabled_test')
        # Default-off (no flag set): the feature is a complete no-op.
        assert get_global_rate_limiter(app) is None
        # Enabled but with no backend URL is also a no-op.
        app.conf.global_rate_limit_enabled = True
        assert get_global_rate_limiter(app) is None
