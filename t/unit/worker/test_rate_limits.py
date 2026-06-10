"""Unit tests for :mod:`celery.worker.rate_limits`.

Covers the opt-in Redis-backed global (cluster-wide) task rate limiter
(feature F-010): the :class:`~celery.worker.rate_limits.RedisTokenBucket`
subclass and the :func:`~celery.worker.rate_limits.get_limiter_client`
factory.

Every test runs with **no real Redis** -- ``redis.Redis`` and the registered
Lua script are replaced with :mod:`unittest.mock` doubles, so no network or
socket connection to Redis is ever attempted.  The single test that needs a
genuine ``redis`` exception class (the fail-open path) guards itself with
``pytest.importorskip('redis')`` so it skips gracefully when redis-py is not
installed, mirroring :mod:`t.unit.backends.test_redis`.
"""
from unittest.mock import MagicMock, patch

import pytest
from kombu.utils.limits import TokenBucket

from celery.exceptions import ImproperlyConfigured
from celery.worker import rate_limits
from celery.worker.rate_limits import RedisTokenBucket, get_limiter_client


class test_RedisTokenBucket:
    """Behavioural coverage for the Redis-coordinated token bucket."""

    def _make_bucket(self, fill_rate, capacity=1,
                     key='celery:rate_limit:celery:tasks.foo',
                     script_return=(1, 0)):
        # Build a RedisTokenBucket backed by a fully mocked redis client so no
        # real connection is opened.  ``register_script`` returns a callable
        # mock whose ``return_value`` is the ``(allowed, wait_ms)`` pair the
        # Lua script would return; that same mock is exactly ``bucket._script``
        # (``__init__`` stores ``client.register_script(TOKEN_BUCKET_LUA)``).
        client = MagicMock(name='redis_client')
        client.register_script.return_value.return_value = list(script_return)
        bucket = RedisTokenBucket(fill_rate, capacity=capacity,
                                  client=client, key=key)
        return bucket, client

    # -- Case 1: script invocation & key scheme ----------------------------

    def test_can_consume_runs_script_with_expected_args(self):
        bucket, client = self._make_bucket(
            10.0, capacity=1, key='celery:rate_limit:celery:tasks.foo',
            script_return=(1, 0))

        result = bucket.can_consume(1)

        # The registered Lua script is invoked exactly once with the bucket's
        # key and the (fill_rate, capacity, ttl, tokens) argument vector.
        bucket._script.assert_called_once_with(
            keys=[bucket.key],
            args=[bucket.fill_rate, bucket.capacity, bucket._ttl, 1],
        )
        assert result is True
        assert isinstance(result, bool)

        # ``capacity=1`` is coerced to ``1.0`` via float() and the TTL floors
        # at MIN_BUCKET_TTL (max(60, int(1.0 / 10.0 * 2)) == 60).
        assert bucket.fill_rate == 10.0
        assert bucket.capacity == 1.0
        assert bucket._ttl == 60

        # The script body is registered exactly once, at construction time.
        client.register_script.assert_called_once_with(
            rate_limits.TOKEN_BUCKET_LUA)

    def test_can_consume_returns_false_when_denied(self):
        bucket, _ = self._make_bucket(10.0, script_return=(1, 0))
        # Simulate an empty bucket: not allowed, 1500 ms until next refill.
        bucket._script.return_value = [0, 1500]
        assert bucket.can_consume(1) is False

    def test_key_follows_documented_scheme(self):
        bucket, _ = self._make_bucket(
            10.0, key='celery:rate_limit:celery:tasks.foo')
        # Documented shape: ``celery:rate_limit:{app}:{task_name}``.
        assert bucket.key == 'celery:rate_limit:celery:tasks.foo'
        assert bucket.key.startswith('celery:rate_limit:')
        assert len(bucket.key.split(':')) == 4

    # -- Case 2: subclass / inheritance (queue protocol not overridden) -----

    def test_is_tokenbucket_subclass(self):
        assert issubclass(RedisTokenBucket, TokenBucket)

    def test_queue_protocol_methods_are_inherited(self):
        # The request-queue protocol used by
        # ``Consumer._schedule_bucket_request`` must be the *unmodified* kombu
        # implementation -- identity proves it is inherited, not overridden.
        assert RedisTokenBucket.add is TokenBucket.add
        assert RedisTokenBucket.pop is TokenBucket.pop
        assert RedisTokenBucket.clear_pending is TokenBucket.clear_pending

    def test_inherited_deque_queue_still_works(self):
        bucket, _ = self._make_bucket(10.0)
        bucket.add(('req', 1))
        assert bucket.contents
        assert bucket.pop() == ('req', 1)
        bucket.clear_pending()
        assert not bucket.contents

    # -- Case 3: expected_time caching + sub-1/s positivity -----------------

    def test_expected_time_returns_cached_wait_without_extra_call(self):
        bucket, _ = self._make_bucket(10.0, script_return=(0, 1500))
        assert bucket.can_consume(1) is False
        count = bucket._script.call_count
        assert count == 1
        # 1500 ms cached by ``can_consume`` -> 1.5 s, and ``expected_time``
        # performs no additional Redis round trip.
        assert bucket.expected_time(1) == 1.5
        assert bucket._script.call_count == count

    def test_sub_one_per_second_rate_has_positive_expected_time(self):
        # ``"1/m"`` -> fill_rate 1/60; the Lua ``math.ceil`` guarantees a
        # strictly positive wait so the consumer never hot-loops rescheduling.
        bucket, _ = self._make_bucket(1 / 60, script_return=(0, 60000))
        bucket.can_consume(1)
        assert bucket.expected_time(1) > 0

    # -- Case 6: fail-open behaviour (real redis error, throttled warning) ---

    def test_fail_open_on_redis_error_logs_throttled_warning(self):
        redis = pytest.importorskip('redis')
        bucket, _ = self._make_bucket(
            10.0, key='celery:rate_limit:celery:tasks.send_sms')
        # A genuine redis error (subclass of RedisError) is what the limiter
        # catches; a plain Exception would not be caught.
        bucket._script.side_effect = redis.exceptions.ConnectionError('boom')

        with patch('celery.worker.rate_limits.logger') as mock_logger:
            result = bucket.can_consume(1)

            # Must not raise: it fails open to a local shadow bucket whose
            # fresh capacity-1 state admits the first request.
            assert isinstance(result, bool)
            assert result is True

            # Exactly one warning is emitted for the first failure.
            assert mock_logger.warning.called
            assert mock_logger.warning.call_count == 1

            # The warning references only the task name and the exception type.
            warning_args = str(mock_logger.warning.call_args)
            assert 'tasks.send_sms' in warning_args
            assert 'ConnectionError' in warning_args

            # SECURITY: no Redis URL or embedded credentials may leak.
            assert 'redis://' not in warning_args
            assert 'rediss://' not in warning_args

            # Throttle: an immediate second failure is suppressed by the
            # WARN_THROTTLE window, so the warning count stays at one.  The
            # second decision's bool is intentionally not asserted (the local
            # shadow may have drained its single token by now).
            bucket.can_consume(1)
            assert mock_logger.warning.call_count == 1


class test_get_limiter_client:
    """Coverage for the cached, URL-resolving Redis client factory."""

    def _redis_mock(self):
        # Stand-in for the ``redis`` module exposing ``Redis.from_url``, which
        # returns a distinct sentinel client object.
        mod = MagicMock(name='redis_module')
        mod.Redis.from_url.return_value = MagicMock(name='client')
        return mod

    # -- Case 4: ImproperlyConfigured paths ---------------------------------

    def test_raises_when_redis_not_installed(self):
        # ``self.app`` is a fresh, un-cached app, so the redis-missing branch
        # is reached (mirrors test_redis.py::test_no_redis).
        with patch('celery.worker.rate_limits.redis', None):
            with pytest.raises(ImproperlyConfigured):
                get_limiter_client(self.app)

    def test_raises_on_non_redis_scheme(self):
        # Patch redis to a truthy mock module so the redis-missing branch is
        # skipped, then force a non-redis URL with the highest precedence.
        with patch('celery.worker.rate_limits.redis', MagicMock()):
            self.app.conf.worker_rate_limit_url = 'amqp://guest@localhost//'
            self.app.conf.result_backend = 'cache+memory://'
            self.app.conf.broker_url = 'memory://'
            with pytest.raises(ImproperlyConfigured):
                get_limiter_client(self.app)

    def test_raises_when_no_redis_url_resolvable(self):
        # No explicit limiter URL and both the default broker (memory://) and
        # result backend (cache+memory://) are non-redis -> nothing resolves.
        with patch('celery.worker.rate_limits.redis', MagicMock()):
            app = self.Celery(set_as_current=False)
            assert app.conf.worker_rate_limit_url is None
            with pytest.raises(ImproperlyConfigured):
                get_limiter_client(app)

    # -- Case 5: client resolution precedence + caching ---------------------

    def test_url_precedence_worker_rate_limit_url_wins(self):
        mod = self._redis_mock()
        with patch('celery.worker.rate_limits.redis', mod):
            app = self.Celery(set_as_current=False)
            app.conf.worker_rate_limit_url = 'redis://h1'
            get_limiter_client(app)
            assert mod.Redis.from_url.call_args.args[0] == 'redis://h1'

    def test_url_precedence_falls_back_to_result_backend(self):
        mod = self._redis_mock()
        with patch('celery.worker.rate_limits.redis', mod):
            app = self.Celery(set_as_current=False)
            # worker_rate_limit_url unset -> the redis result_backend wins.
            app.conf.result_backend = 'redis://h2'
            get_limiter_client(app)
            assert mod.Redis.from_url.call_args.args[0] == 'redis://h2'

    def test_url_precedence_falls_back_to_broker_url(self):
        mod = self._redis_mock()
        with patch('celery.worker.rate_limits.redis', mod):
            app = self.Celery(set_as_current=False)
            # worker_rate_limit_url unset and result_backend is the default
            # non-redis cache+memory:// -> the redis broker_url wins.
            app.conf.broker_url = 'redis://h3'
            get_limiter_client(app)
            assert mod.Redis.from_url.call_args.args[0] == 'redis://h3'
            # The short socket timeouts are forwarded to redis-py so a slow
            # Redis can never stall the eventlet/gevent hub.
            assert mod.Redis.from_url.call_args.kwargs == {
                'socket_timeout': rate_limits.SOCKET_TIMEOUT,
                'socket_connect_timeout': rate_limits.SOCKET_TIMEOUT,
            }

    def test_redis_socket_scheme_is_mapped_to_unix(self):
        mod = self._redis_mock()
        with patch('celery.worker.rate_limits.redis', mod):
            app = self.Celery(set_as_current=False)
            # ``redis+socket://`` is an accepted scheme that redis-py only
            # understands as ``unix://``; the factory rewrites it transparently.
            app.conf.worker_rate_limit_url = 'redis+socket:///var/run/redis.sock'
            get_limiter_client(app)
            assert (mod.Redis.from_url.call_args.args[0]
                    == 'unix:///var/run/redis.sock')

    def test_client_is_cached_per_app(self):
        mod = self._redis_mock()
        with patch('celery.worker.rate_limits.redis', mod):
            app = self.Celery(set_as_current=False)
            app.conf.worker_rate_limit_url = 'redis://h1'
            c1 = get_limiter_client(app)
            c2 = get_limiter_client(app)
            # Exactly one client is built per app and reused on every call.
            assert c1 is c2
            assert mod.Redis.from_url.call_count == 1
