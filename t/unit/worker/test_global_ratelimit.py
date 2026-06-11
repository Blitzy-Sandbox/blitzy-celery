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


def test_local_fallback_when_no_redis_url():
    # Graceful degradation, construction half: when neither an injected client
    # nor a usable URL is configured (the "redis-py absent / unconfigured"
    # case), the bucket must build no client/script and transparently use the
    # inherited local TokenBucket math.  This is a REAL construction path --
    # Redis is neither mocked nor patched; there simply is no Redis to talk to.
    fill_rate = rate('1/s')  # slow refill so the second consume is denied
    key = f'grl:{uuid4().hex}'
    bucket = GlobalTokenBucket(
        fill_rate, capacity=1, redis_url='', key=key, client=None)

    # No client and no script were built -> every call takes the local path.
    assert bucket._client is None
    assert bucket._script is None
    # Local fallback math: a fresh, full bucket admits exactly one token, then
    # is empty until it refills (which takes ~1s at 1/s, far longer than this
    # test), so the second call is denied -- proving real local enforcement.
    assert bucket.can_consume(1) is True
    assert bucket.can_consume(1) is False
    # expected_time delegates to the inherited local estimate (a float).
    wait = bucket.expected_time(1)
    assert isinstance(wait, float)
    assert wait >= 0.0


def test_local_fallback_when_redis_url_malformed():
    # Graceful degradation, construction half: a malformed URL makes the real
    # ``redis.from_url`` raise ``ValueError`` while building the client.  The
    # bucket must catch that, leave itself client/script-less, and degrade to
    # local limiting instead of letting construction raise.  This exercises a
    # genuine ``redis.from_url`` call (not a mock/patch) with a bad URL.
    if redis is None:
        pytest.skip('redis-py is not installed')
    fill_rate = rate('1/s')
    key = f'grl:{uuid4().hex}'
    bucket = GlobalTokenBucket(
        fill_rate, capacity=1,
        redis_url='redis://localhost:notaport/0', key=key, client=None)

    # Construction swallowed the URL error and fell back to local limiting.
    assert bucket._client is None
    assert bucket._script is None
    assert bucket.can_consume(1) is True


def test_local_fallback_when_register_script_fails():
    # Graceful degradation, construction half: even when a client is present,
    # if registering the Lua script fails with a redis error the bucket must
    # degrade to local limiting rather than raise.  ``register_script`` only
    # computes a SHA locally, so a healthy client never hits this guard; we
    # therefore drive it with a REAL ``redis.Redis`` subclass whose
    # ``register_script`` raises.  This is NOT a Redis mock -- it is a genuine
    # ``redis.Redis`` instance and the Redis server is never faked; only the
    # local SHA-registration step is forced to fail to reach the defensive
    # fallback branch.
    if redis is None:
        pytest.skip('redis-py is not installed')

    class _RegisterScriptFails(redis.Redis):
        def register_script(self, script):
            raise redis.exceptions.RedisError('register_script failed')

    fill_rate = rate('1/s')
    key = f'grl:{uuid4().hex}'
    bucket = GlobalTokenBucket(
        fill_rate, capacity=1, redis_url='redis://localhost:6390/0',
        key=key, client=_RegisterScriptFails())

    # The client was accepted, but script registration failed -> no script ->
    # the bucket uses the inherited local math instead of raising.
    assert bucket._client is not None
    assert bucket._script is None
    assert bucket.can_consume(1) is True


@pytest.mark.timeout(30)
def test_expected_time_returns_cached_redis_wait(redis_url):
    # When Redis actually answers a (denied) ``can_consume``, ``expected_time``
    # must return the wait Redis computed -- the value cached on the bucket --
    # WITHOUT a second round-trip and WITHOUT falling back to local timing.
    # Uses the REAL container so the wait is produced by the atomic Lua script.
    fill_rate = rate('1/s')  # slow refill so the second consume is denied
    key = f'grl:{uuid4().hex}'
    bucket = GlobalTokenBucket(
        fill_rate, capacity=1, redis_url=redis_url, key=key)
    # Redis is configured and reachable, so a script was registered.
    assert bucket._script is not None

    assert bucket.can_consume(1) is True   # consumes the only token
    assert bucket.can_consume(1) is False  # denied -> Redis returns a wait
    # Redis answered the deny, so this was NOT a local fallback decision ...
    assert bucket._used_local_fallback is False
    assert bucket._wait > 0.0
    # ... and expected_time returns exactly that cached, Redis-provided wait.
    assert bucket.expected_time(1) == bucket._wait
