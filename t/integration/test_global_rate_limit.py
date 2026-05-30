"""Live-Redis end-to-end and direct-limiter tests for the global rate limiter.

This module proves that a task's ``rate_limit`` attribute is enforced
*cluster-wide* via the Redis-backed :class:`~celery.utils.rate_limit.GlobalRateLimiter`
rather than per-worker-process, and adds targeted coverage of the limiter's
rate-string parsing, graceful Redis-failure fallback, cluster-wide key sharing,
and opt-out behaviours.

The single session worker provided by the integration harness still proves the
per-second cap (C.1/C.2). The genuinely *cluster-wide* property -- multiple
consumers sharing one Redis bucket -- is proven directly and deterministically
by C.3 (two ``GlobalRateLimiter`` instances on one key), so no second worker
process is required. The graceful fallback (C.5) and the factory opt-out (C.6)
together guarantee the No-Regressions mandate: disabling the flag or losing
Redis degrades to the exact legacy ``TokenBucket`` behaviour with no exceptions
surfacing to the consumer.

Authoritative AAP refs: 0.5.2.13, 0.6.1.4 (Group D), 0.7.5 (Testing
Conventions), I1, I2, I6.
"""
import time
from unittest.mock import MagicMock

import pytest
from kombu.utils.limits import TokenBucket

from celery import Celery
from celery.utils.rate_limit import GlobalRateLimiter, get_rate_limiter_for_task
from celery.utils.time import rate

from .conftest import get_redis_connection
from .tasks import rate_limited_task

# Mirrors t/integration/test_tasks.py: bounds the per-result ``.get(timeout=...)``
# wait for the live-worker end-to-end tests.
TIMEOUT = 10


# The whole class is gated on a live Redis (broker and/or backend) via the
# ``redis`` marker, which CI provides through TEST_BROKER/TEST_BACKEND. The
# ``redis`` marker is registered centrally in ``pyproject.toml`` -- it is NOT
# registered here.
#
# MARKER DISCIPLINE (verified gotcha -- see AAP Phase B): only ``redis``,
# ``flaky`` and ``timeout`` are registered markers in this repo. The
# unregistered Celery marker is intentionally NOT used:
# ``celery.contrib.pytest`` is not registered as a pytest plugin (no
# ``pytest11`` entry point in ``setup.py``, no ``pytest_plugins``
# declaration), so that marker is unknown. Under the project's
# ``--strict-markers`` (set in ``[tool.pytest.ini_options].addopts``) applying
# it would raise a collection error ("'celery' not found in markers
# configuration option"). Neighbouring integration suites (test_tasks.py,
# test_canvas.py, test_inspect.py) likewise rely only on
# ``flaky``/``timeout``/``xfail``.
@pytest.mark.redis
class test_global_rate_limit:
    """End-to-end + direct-limiter coverage for the Redis global rate limiter."""

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.timeout(15)
    def test_rate_limit_is_enforced_cluster_wide(self, manager):
        """A ``rate_limit='2/s'`` task is throttled across the cluster.

        Submitting ~10 invocations near-instantaneously must take noticeably
        longer than 1s to complete: the 2/s cap throttles dispatch. Without the
        limiter all 10 would finish in well under a second.
        """
        n = 10
        start = time.monotonic()
        results = [rate_limited_task.delay() for _ in range(n)]
        values = [r.get(timeout=TIMEOUT) for r in results]
        elapsed = time.monotonic() - start
        assert all(v == 'ok' for v in values)
        # 10 calls at 2/s: bucket capacity 2 grants a 2-token initial burst,
        # then ~8 more arrive at 2/s ~= 4.0-4.5s of throttling. A >= 4.0s floor
        # cleanly distinguishes the throttled (~4s) path from the un-throttled
        # (<1s) path. ``@pytest.mark.flaky`` reruns absorb timer jitter.
        assert elapsed >= 4.0

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.timeout(15)
    def test_single_worker_throughput_is_capped(self, manager):
        """A smaller burst still observes the per-second cap on one worker."""
        n = 6
        start = time.monotonic()
        results = [rate_limited_task.delay() for _ in range(n)]
        for r in results:
            assert r.get(timeout=TIMEOUT) == 'ok'
        elapsed = time.monotonic() - start
        # 6 calls at 2/s with a capacity-2 burst: (n - capacity) / rate =
        # (6 - 2) / 2 = 2.0s of throttling. A >= 2.0s floor proves the cap.
        assert elapsed >= 2.0

    def test_two_limiters_share_one_redis_key(self):
        """Two limiters keyed on the same task share ONE Redis bucket.

        This is the deterministic proof of the cluster-wide property: a
        per-process ``TokenBucket`` would let each instance independently
        consume ``capacity`` tokens (~10 total), whereas the two
        ``GlobalRateLimiter`` instances draw from the single shared
        ``celery:rate:<task>`` budget.
        """
        client = get_redis_connection()
        task_name = 'test.shared.bucket'
        key = f'celery:rate:{task_name}'
        # Clean slate so prior runs cannot leak state into this assertion
        # (the script's TTL also self-cleans the key over time).
        client.delete(key)
        try:
            capacity = 5
            a = GlobalRateLimiter(5.0, client, task_name, capacity=capacity)
            b = GlobalRateLimiter(5.0, client, task_name, capacity=capacity)
            allowed = 0
            for _ in range(20):
                if a.can_consume(1):
                    allowed += 1
                if b.can_consume(1):
                    allowed += 1
            # Both instances consume from the SAME celery:rate:<task> bucket, so
            # the combined burst is bounded by the single shared capacity (plus
            # negligible refill during the tight loop), proving sharing.
            assert 1 <= allowed <= capacity + 1
        finally:
            client.delete(key)

    def test_rate_string_parsing(self):
        """``rate()`` units parse correctly and flow into the limiter state."""
        assert rate('5/s') == 5.0
        assert round(rate('100/m'), 4) == 1.6667
        assert round(rate('1000/h'), 4) == 0.2778
        # The parsed fill_rate is carried through to the inherited TokenBucket
        # state. A MagicMock client keeps this method free of live Redis (the
        # constructor only calls ``register_script`` on it).
        client = MagicMock()
        for spec, expected in [
            ('5/s', 5.0),
            ('100/m', 100 / 60.0),
            ('1000/h', 1000 / 3600.0),
        ]:
            limiter = GlobalRateLimiter(rate(spec), client, 'test.parse', capacity=5)
            assert limiter.fill_rate == expected

    def test_fallback_to_in_memory_when_redis_raises(self):
        """Any Redis error degrades to the in-memory parent bucket (R5).

        The limiter must NEVER let a Redis exception escape: it logs a WARNING
        and delegates to ``super()`` (the in-memory ``TokenBucket``) for that
        single call. This is the failure-isolation guarantee that keeps the
        consumer's task-dispatch loop alive when Redis is unreachable.
        """
        client = MagicMock()
        # The registered script raises on invocation, simulating a dead Redis.
        script = MagicMock(side_effect=ConnectionError('redis down'))
        client.register_script.return_value = script
        limiter = GlobalRateLimiter(5.0, client, 'test.fallback', capacity=5)
        # Must NOT raise -- falls back to the in-memory parent TokenBucket,
        # which starts full (``_tokens == capacity``) so the first call passes.
        result = limiter.can_consume(1)
        assert result is True
        # ``expected_time`` shares the same graceful-fallback contract and the
        # parent implementation always returns a float (seconds).
        wait = limiter.expected_time(1)
        assert isinstance(wait, float)

    def test_opt_out_returns_plain_token_bucket(self):
        """``worker_global_rate_limit_enabled=False`` yields legacy behaviour.

        With the flag disabled the factory short-circuits BEFORE Redis discovery
        and returns the legacy per-process ``TokenBucket`` -- never the
        ``GlobalRateLimiter`` -- even though this app declares a Redis backend.
        """
        # A throwaway app so the shared session app's conf is not mutated and
        # state cannot leak into other tests.
        app = Celery('opt_out_test', broker='redis://', backend='redis://')
        app.conf.worker_global_rate_limit_enabled = False

        @app.task(rate_limit='5/s')
        def some_task():
            return 'ok'

        bucket = get_rate_limiter_for_task(app, some_task)
        assert isinstance(bucket, TokenBucket)
        assert not isinstance(bucket, GlobalRateLimiter)

    def test_factory_selects_global_limiter_with_redis(self, manager):
        """In the live Redis env the factory selects the GlobalRateLimiter.

        The integration harness defaults ``TEST_BACKEND`` to ``redis://`` (a
        ``RedisBackend``), so the factory must pick the cluster-wide limiter.
        The guard mirrors the factory's selection contract -- a Redis result
        backend *or* a ``redis://``/``rediss://`` broker -- so the assertion is
        correct under a Redis-broker-only variant too, and still passes on a
        fully non-Redis CI variant by asserting the legacy ``TokenBucket``.
        """
        app = manager.app
        assert app.conf.worker_global_rate_limit_enabled is True
        bucket = get_rate_limiter_for_task(app, rate_limited_task)
        # Deferred/local import mirrors the factory's own lazy import and avoids
        # a load-time cycle with the backend layer.
        from celery.backends.redis import RedisBackend
        # Mirror the factory's selection contract exactly (see
        # _discover_redis_client in celery/utils/rate_limit/factory.py): the
        # GlobalRateLimiter is chosen when the result backend is a RedisBackend
        # OR the broker URL is ``redis://``/``rediss://``. The broker scheme MUST
        # be included so a Redis-broker-only environment is not misclassified.
        redis_env = isinstance(app.backend, RedisBackend) or str(
            app.conf.broker_url or '').startswith(('redis://', 'rediss://'))
        if redis_env:
            assert isinstance(bucket, GlobalRateLimiter)
        else:
            assert isinstance(bucket, TokenBucket)
            assert not isinstance(bucket, GlobalRateLimiter)
