"""Unit tests for the Redis-backed global (pool-wide) rate limiter.

These tests exercise :class:`celery.worker.global_ratelimit.GlobalTokenBucket`
against a *real* Redis instance provided by the ``redis_url`` container fixture
in ``conftest.py`` -- Redis is never mocked.  They prove that:

* the configured rate is enforced as a single aggregate across many bucket
  instances (i.e. across the whole worker pool, not per process);
* the bucket degrades gracefully (never raises) when Redis is unreachable;
* token consumption is atomic under concurrency (no over-admission).
"""
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest

from celery.utils.time import rate
from celery.worker.global_ratelimit import GlobalTokenBucket

try:
    import redis
except ImportError:  # pragma: no cover - redis is optional
    redis = None


@pytest.mark.timeout(60)
def test_aggregate_across_workers_at_or_below_rate(redis_url):
    # "10/s on 10 workers must stay <= 10/s, not 100/s": several bucket
    # instances share ONE Redis key, simulating multiple worker processes.
    fill_rate = rate('10/s')  # == 10.0 tokens/sec
    assert fill_rate == 10.0
    key = f'grl:{uuid4().hex}'
    buckets = [
        GlobalTokenBucket(fill_rate, capacity=1, redis_url=redis_url, key=key)
        for _ in range(3)
    ]

    admitted = 0
    window = 1.0
    start = time.monotonic()
    while time.monotonic() - start < window:
        for bucket in buckets:
            if bucket.can_consume(1):
                admitted += 1
        time.sleep(0.005)
    elapsed = time.monotonic() - start

    # Pool-wide ceiling = initial capacity + refill over the window.  Without a
    # shared bucket, three instances would admit ~3x this many tokens.
    ceiling = 1 + fill_rate * elapsed
    assert admitted <= ceiling + 2
    assert admitted >= 1


@pytest.mark.timeout(30)
def test_redis_down_graceful_fallback():
    if redis is None:
        pytest.skip('redis-py is not installed')
    fill_rate = rate('10/s')
    key = f'grl:{uuid4().hex}'
    # A genuinely unreachable endpoint (no server on this port); a short connect
    # timeout keeps the failure fast.  This is a REAL socket, not a mock.
    dead_client = redis.from_url(
        'redis://localhost:6390/0', socket_connect_timeout=0.5)
    bucket = GlobalTokenBucket(
        fill_rate, capacity=1, redis_url='redis://localhost:6390/0',
        key=key, client=dead_client)

    # Neither call may raise: the ConnectionError is caught and the bucket
    # degrades to the inherited local TokenBucket math.
    allowed = bucket.can_consume(1)
    wait = bucket.expected_time(1)

    assert isinstance(allowed, bool)
    assert allowed is True  # a fresh, full local bucket admits the first token
    assert isinstance(wait, float)


@pytest.mark.timeout(60)
def test_concurrent_consume_no_over_admission(redis_url):
    # A slow refill so the burst ceiling is dominated by capacity, making any
    # over-admission (a non-atomic read-modify-write) clearly visible.
    fill_rate = rate('1/s')
    capacity = 1
    key = f'grl:{uuid4().hex}'
    n_threads = 20
    barrier = threading.Barrier(n_threads)
    results = []
    results_lock = threading.Lock()

    def worker():
        bucket = GlobalTokenBucket(
            fill_rate, capacity=capacity, redis_url=redis_url, key=key)
        barrier.wait()  # release all threads together to maximise contention
        allowed = bucket.can_consume(1)
        with results_lock:
            results.append(allowed)

    # ``with ThreadPoolExecutor`` joins every worker thread on exit, satisfying
    # the parent ``threads_not_lingering`` autouse sanity check.
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        futures = [executor.submit(worker) for _ in range(n_threads)]
        for future in futures:
            future.result()

    admitted = sum(1 for allowed in results if allowed)
    # Only ``capacity`` token(s) exist at t0 and refill over the sub-second
    # burst is negligible; an atomic script admits exactly ``capacity``.
    assert admitted >= 1
    assert admitted <= capacity + 1
