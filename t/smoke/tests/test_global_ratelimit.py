from __future__ import annotations

import time

import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup, CeleryTestWorker, CeleryWorkerCluster, RedisContainer

from celery import Celery
from t.integration.tasks import add


class test_global_rate_limit:
    """End-to-end smoke test for the Redis-backed global (pool-wide) rate limiter.

    The configured ``rate_limit`` must be enforced as a single aggregate ceiling
    across the ENTIRE worker pool, not independently within each worker process.
    With two workers and the global limiter enabled, completing ``NUM_TASKS``
    tasks takes ~= ``NUM_TASKS / RATE`` seconds; without it, two independent
    per-worker buckets would roughly double the aggregate rate.
    """

    RATE = 10.0          # tokens/second -- matches the "10/s" configured below
    CAPACITY = 1         # the consumer always builds the bucket with capacity=1
    NUM_TASKS = 30       # exceeds one refill window yet bounded for CI

    @pytest.fixture
    def default_worker_app(
        self,
        default_worker_app: Celery,
        redis_test_container: RedisContainer,
    ) -> Celery:
        app = default_worker_app
        # Enable the opt-in Redis-backed pool-wide rate limiter on the workers.
        app.conf.worker_global_rate_limit = True
        # Use the IN-NETWORK hostname (NOT localhost): the workers run inside the
        # Docker network, so they must reach Redis via its network hostname:6379.
        # ``redis_test_container.port`` is the host-exposed port and is only valid
        # for host-side localhost access -- it must NOT be used here.
        app.conf.worker_global_rate_limit_url = (
            f"redis://{redis_test_container.hostname}:6379/0"
        )
        # Apply the rate limit WITHOUT editing t/smoke/tasks.py (minimal-change).
        app.conf.task_annotations = {
            "t.integration.tasks.add": {"rate_limit": "10/s"},
        }
        # Deterministic dispatch: one prefetched task at a time per worker.
        app.conf.worker_prefetch_multiplier = 1
        app.conf.worker_concurrency = 1
        return app

    @pytest.fixture
    def celery_worker_cluster(
        self,
        celery_worker: CeleryTestWorker,
        celery_other_dev_worker: CeleryTestWorker,
    ) -> CeleryWorkerCluster:
        cluster = CeleryWorkerCluster(celery_worker, celery_other_dev_worker)
        yield cluster
        cluster.teardown()

    @pytest.mark.timeout(120)
    def test_pool_wide_rate_limit(self, celery_setup: CeleryTestSetup):
        # Two distinct real workers, each with its own queue, sharing one app,
        # broker and backend -- a genuine 2-worker pool.
        queues = [worker.worker_queue for worker in celery_setup.worker_cluster]
        assert len(queues) == 2
        assert queues[0] != queues[1]

        # Spread the load across BOTH queues so both workers actively compete
        # for the SAME shared Redis bucket (keyed by the task name) -- exactly
        # the pool-wide scenario under test.
        results = []
        start = time.monotonic()
        for i in range(self.NUM_TASKS):
            queue = queues[i % 2]
            results.append(add.s(i, i).set(queue=queue).apply_async())

        # Await every result; the wall-clock window spans the whole pool.
        for i, result in enumerate(results):
            assert result.get(timeout=RESULT_TIMEOUT) == i + i
        elapsed = time.monotonic() - start

        # Pool-wide ceiling: the aggregate admitted rate must NOT scale with the
        # worker count. With the global limiter, observed_rate ~= RATE (10/s).
        # WITHOUT it, two per-worker buckets give ~= 2*RATE (~20/s) and finish in
        # roughly half the time, so observed_rate would exceed RATE * 1.5.
        observed_rate = len(results) / elapsed
        assert observed_rate <= self.RATE * 1.5, (
            observed_rate,
            elapsed,
            len(results),
        )
        # Equivalent count-form bound: admitted <= capacity + rate*elapsed + rate.
        assert len(results) <= self.CAPACITY + self.RATE * elapsed + self.RATE, (
            elapsed,
            len(results),
        )
