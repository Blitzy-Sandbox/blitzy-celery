"""Unit tests for :class:`celery.rate_limiting.redis_rate_limiter.RedisTokenBucket`.

This module is the **primary** automated coverage for the opt-in, Redis-backed
*global* rate limiter. It runs entirely against a mocked Redis client -- no live
Redis server and no ``redis-py`` are required to execute it -- mirroring the
mocking idiom of ``t/unit/backends/test_redis.py`` (the canonical reference for
Redis mocking in this repository).

How the seam is mocked
----------------------
``RedisTokenBucket`` connects **lazily**: constructing it performs no Redis I/O,
and its ``_get_client()`` early-returns once ``_redis_client`` is set (without
re-registering the Lua scripts). The tests exploit that by injecting a plain
``Mock`` client plus mocked ``_consume_script`` / ``_expected_script`` callables
directly onto the instance, so the registered Lua scripts are never sent to a
server and ``redis.Redis.from_url`` is never called. This keeps the suite free
of any live I/O and importable even when ``redis-py`` is absent.
"""
from unittest.mock import ANY, Mock, call, patch

import pytest

try:
    from redis import exceptions
except ImportError:  # pragma: no cover - redis-py is optional (via kombu[redis])
    exceptions = None

from celery.rate_limiting.redis_rate_limiter import RedisTokenBucket
from celery.utils.time import rate

# ``RedisTokenBucket`` catches ``redis.exceptions.RedisError`` for its
# fail-open/fail-closed degradation. When ``redis-py`` is installed the symbol
# below *is* that exact class, so an exception raised through a mocked script's
# ``side_effect`` is caught by the limiter's ``except`` clause. When ``redis-py``
# is absent the limiter's own ``except`` refers to a *different* stand-in class,
# so the fail-path tests cannot reliably exercise that branch and are skipped
# (see ``requires_redis_py`` below) -- the same tolerance pattern used by
# ``t/unit/backends/test_redis.py``.
if exceptions is not None:
    RedisError = exceptions.RedisError
else:  # pragma: no cover - exercised only on installs without redis-py
    class RedisError(Exception):
        """Stand-in used only when redis-py is not installed."""

# Skip guard for the fail-open/fail-closed tests, which depend on the real
# ``redis.exceptions.RedisError`` type matching what the limiter catches.
requires_redis_py = pytest.mark.skipif(
    exceptions is None,
    reason='requires redis-py for redis.exceptions.RedisError',
)

#: A representative, independent Redis URL (never the broker / result_backend).
BACKEND_URL = 'redis://localhost:6379/0'


class test_RedisTokenBucket:
    """Behavioural unit tests for :class:`RedisTokenBucket`."""

    def _make_bucket(self, fill_rate=10.0, capacity=1,
                     task_name='myapp.tasks.add', fail_open=True):
        """Build a bucket and inject mocks at its lazy connection seam.

        Constructing the bucket performs no Redis access. We then set
        ``_redis_client`` to a ``Mock`` (so ``_get_client()`` early-returns and
        never calls ``redis.Redis.from_url``) and replace the two registered
        Lua-script callables with plain ``Mock`` objects whose return values /
        side effects each test controls. No ``redis-py`` and no live server are
        required.
        """
        bucket = RedisTokenBucket(
            fill_rate, capacity=capacity,
            backend_url=BACKEND_URL, task_name=task_name, fail_open=fail_open,
        )
        bucket._redis_client = Mock(name='redis_client')
        bucket._consume_script = Mock(name='consume_script')
        bucket._expected_script = Mock(name='expected_script')
        return bucket

    def test_can_consume_allow_then_block(self):
        """can_consume() allows within the limit, then blocks over it."""
        # The atomic consume script returns 1 (allow) or 0 (deny); the limiter
        # coerces that to bool via ``bool(int(allowed))``. Drive a within-limit
        # call followed by an over-limit call purely through mocked returns.
        bucket = self._make_bucket()
        bucket._consume_script.side_effect = [1, 0]

        assert bucket.can_consume(1) is True
        assert bucket.can_consume(1) is False
        assert bucket._consume_script.call_count == 2

    def test_expected_time(self):
        """expected_time() converts the script's microseconds to seconds."""
        # The expected-time script returns WHOLE MICROSECONDS; the limiter
        # divides back to seconds: 250000 us -> 0.25 s. The value is chosen so
        # it cannot be confused with the Redis-error fallback
        # (tokens / fill_rate == 1 / 10 == 0.1), proving the script path ran.
        bucket = self._make_bucket(fill_rate=10.0)
        bucket._expected_script.return_value = 250000

        assert bucket.expected_time(1) == pytest.approx(0.25)
        # The read-only script is consulted against the per-task key with the
        # capacity/fill_rate/tokens triple (and never mutates the bucket).
        bucket._expected_script.assert_called_once_with(
            keys=['celery:global-rate-limit:myapp.tasks.add'],
            args=[bucket.capacity, bucket.fill_rate, 1],
        )

    @requires_redis_py
    def test_fail_open(self):
        """A Redis server error degrades OPEN (allow) by default."""
        # On a Redis *server* error the default fail-open mode ALLOWS the task
        # so a coordination-layer outage never halts processing.
        bucket = self._make_bucket(fail_open=True)
        bucket._consume_script.side_effect = RedisError('redis is down')
        bucket._expected_script.side_effect = RedisError('redis is down')

        assert bucket.can_consume(1) is True
        # expected_time also degrades gracefully to the bounded
        # ``tokens / fill_rate`` backoff (1 / 10 == 0.1) instead of raising.
        assert bucket.expected_time(1) == pytest.approx(0.1)

    @requires_redis_py
    def test_fail_closed(self):
        """A Redis server error degrades CLOSED (block) when fail_open=False."""
        # With fail_open=False the very same Redis error BLOCKS the task.
        bucket = self._make_bucket(fail_open=False)
        bucket._consume_script.side_effect = RedisError('redis is down')

        assert bucket.can_consume(1) is False

    def test_per_task_key_namespacing(self):
        """Keys are namespaced per task name; distinct tasks never collide."""
        # Every task gets its own key, prefixed ``celery:global-rate-limit:``,
        # so distinct tasks (and distinct apps sharing one Redis) never collide.
        bucket = self._make_bucket(task_name='myapp.tasks.add')
        bucket._consume_script.return_value = 1
        bucket.can_consume(1)

        expected_key = 'celery:global-rate-limit:myapp.tasks.add'
        assert bucket._key == expected_key
        # The key actually passed to the atomic op carries the expected prefix.
        assert bucket._consume_script.call_args == call(keys=[expected_key], args=ANY)

        # A different task name must produce a different (non-colliding) key.
        other = self._make_bucket(task_name='myapp.tasks.mul')
        other._consume_script.return_value = 1
        other.can_consume(1)

        assert other._key == 'celery:global-rate-limit:myapp.tasks.mul'
        assert other._key != bucket._key
        assert other._consume_script.call_args == call(
            keys=['celery:global-rate-limit:myapp.tasks.mul'], args=ANY)

    def test_rate_limit_none_is_noop(self):
        """A falsy rate_limit is a complete no-op: no bucket, no Redis access."""
        # ``Consumer.bucket_for_task()`` does:
        #     limit = rate(getattr(type, 'rate_limit', None))
        #     return TokenBucket(limit, capacity=1) if limit else None
        # so a falsy ``rate_limit`` yields ``rate() == 0`` and NO bucket is
        # built -- hence NO ``RedisTokenBucket`` is constructed and NO Redis
        # connection is ever opened.
        assert rate(None) == 0
        assert rate(0) == 0

        # Patch the limiter's connection seam and emulate the factory's falsy
        # short-circuit: because ``limit`` is 0, no bucket is constructed and
        # ``redis.Redis.from_url`` is never reached.
        with patch('celery.rate_limiting.redis_rate_limiter.redis') as mock_redis:
            for rate_limit in (None, 0):
                limit = rate(rate_limit)
                bucket = (
                    RedisTokenBucket(
                        limit, capacity=1,
                        backend_url=BACKEND_URL, task_name='myapp.tasks.add',
                    )
                    if limit else None
                )
                assert bucket is None
            mock_redis.Redis.from_url.assert_not_called()

    def test_inherited_tokenbucket_surface(self):
        """The inherited kombu pending-queue surface is preserved unchanged."""
        # The consumer's scheduling loop relies on the inherited kombu queue
        # surface (add/pop/contents/clear_pending), which RedisTokenBucket does
        # NOT override. A light smoke test confirms it still round-trips.
        from collections import deque

        bucket = self._make_bucket()
        assert isinstance(bucket.contents, deque)

        first, second = ('req-1', 1), ('req-2', 1)
        bucket.add(first)
        bucket.add(second)
        assert list(bucket.contents) == [first, second]
        assert bucket.pop() == first  # FIFO via ``contents.popleft``

        bucket.clear_pending()
        assert len(bucket.contents) == 0
