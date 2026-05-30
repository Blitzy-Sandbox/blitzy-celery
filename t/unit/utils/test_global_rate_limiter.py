"""Unit tests for the Redis-backed global rate limiter.

Covers both public symbols of :mod:`celery.utils.rate_limit`:

* :class:`~celery.utils.rate_limit.global_limiter.GlobalRateLimiter` -- the
  Redis-Lua-backed :class:`kombu.utils.limits.TokenBucket` subclass, including
  its graceful in-memory fallback when Redis raises (R1, R5, R7, R8).
* :func:`~celery.utils.rate_limit.factory.get_rate_limiter_for_task` -- the
  selection factory returning ``None`` / plain ``TokenBucket`` /
  ``GlobalRateLimiter`` (R2, R6).

Every Redis interaction is mocked with :mod:`unittest.mock`; these tests never
contact a real Redis server or broker.  The factory's deferred
``from celery.backends.redis import RedisBackend`` import is exercised by
injecting a stand-in module into ``sys.modules`` (see
``_fake_redis_backend_module``) so the ``isinstance`` branch resolves against a
dummy class regardless of whether the optional ``redis`` extra is installed.
"""
import sys
import types
from unittest.mock import MagicMock, call, patch

import pytest
from kombu.utils.limits import TokenBucket

from celery.utils.rate_limit import GlobalRateLimiter, get_rate_limiter_for_task
from celery.utils.rate_limit.global_limiter import _LUA_TOKEN_BUCKET

# Dotted path of the module-level logger inside global_limiter.py; patched in the
# fallback tests to assert a WARNING is emitted (and to keep test output quiet).
GLOBAL_LIMITER_LOGGER = 'celery.utils.rate_limit.global_limiter.logger'


def _make_limiter(script_return=None, capacity=5, fill_rate=5.0, task_name='app.add'):
    # The registered script callable is ``client.register_script.return_value``;
    # ``GlobalRateLimiter.__init__`` stores it as ``self._script``.  Control its
    # result via ``.return_value`` and force errors via ``.side_effect``.
    client = MagicMock(name='redis_client')
    if script_return is not None:
        client.register_script.return_value.return_value = script_return
    grl = GlobalRateLimiter(fill_rate, client, task_name, capacity=capacity)
    return grl, client


def _make_app(enabled=True, broker_url='pyamqp://', backend=None):
    app = MagicMock(name='app')
    app.conf.worker_global_rate_limit_enabled = enabled
    app.conf.broker_url = broker_url
    # Default to a plain object() so the factory's isinstance(..., RedisBackend)
    # check is False unless a test supplies a fake RedisBackend instance.
    app.backend = backend if backend is not None else object()
    return app


def _make_task(rate_limit='5/s', name='app.add'):
    # NOTE: use SimpleNamespace, NOT Mock(name=...). Mock(name=...) sets the
    # mock's repr-name, not a `.name` ATTRIBUTE, and `.name` must be a real str
    # because the factory builds the Redis key from it.
    return types.SimpleNamespace(rate_limit=rate_limit, name=name)


def _fake_redis_backend_module():
    # Inject a stand-in for `celery.backends.redis` so the factory's deferred
    # `from celery.backends.redis import RedisBackend` resolves to this dummy
    # class.  This keeps the isinstance branch deterministic regardless of
    # whether the optional `redis` extra is installed in the test environment.
    mod = types.ModuleType('celery.backends.redis')

    class RedisBackend:
        pass

    mod.RedisBackend = RedisBackend
    return mod


class test_GlobalRateLimiter:

    def test_registers_script_once_with_lua_source(self):
        grl, client = _make_limiter()
        assert client.register_script.call_count == 1
        assert client.register_script.call_args == call(_LUA_TOKEN_BUCKET)
        assert grl._script is client.register_script.return_value

    def test_key_naming(self):
        grl, _ = _make_limiter(task_name='app.add')
        assert grl._key == 'celery:rate:app.add'

    def test_can_consume_allowed(self):
        grl, _ = _make_limiter(script_return=[1, 0])
        assert grl.can_consume(1) is True

    def test_can_consume_denied(self):
        grl, _ = _make_limiter(script_return=[0, 1.5])
        assert grl.can_consume(1) is False

    def test_expected_time_returns_float(self):
        grl, _ = _make_limiter(script_return=[0, 0.42])
        result = grl.expected_time(1)
        assert result == 0.42
        assert isinstance(result, float)

    def test_can_consume_invoked_with_keys_and_args(self):
        # ARGV contract is [rate, capacity, tokens, consume]; can_consume passes
        # consume=1 so the Lua script spends a token on a successful check.
        grl, client = _make_limiter(script_return=[1, 0])
        grl.can_consume(1)
        client.register_script.return_value.assert_called_with(
            keys=[grl._key], args=[grl.fill_rate, grl.capacity, 1, 1])

    def test_expected_time_invoked_with_consume_zero(self):
        # expected_time is a NON-consuming peek: it passes consume=0 so the Lua
        # script refills/writes timing state but never decrements the bucket.
        grl, client = _make_limiter(script_return=[0, 0.42])
        grl.expected_time(1)
        client.register_script.return_value.assert_called_with(
            keys=[grl._key], args=[grl.fill_rate, grl.capacity, 1, 0])

    def test_can_consume_falls_back_on_redis_error(self):
        grl, client = _make_limiter()
        client.register_script.return_value.side_effect = ConnectionError('boom')
        with patch(GLOBAL_LIMITER_LOGGER) as log:
            result = grl.can_consume(1)
        assert result is True            # fresh bucket has `capacity` tokens
        assert log.warning.called

    def test_expected_time_falls_back_on_redis_error(self):
        grl, client = _make_limiter()
        client.register_script.return_value.side_effect = RuntimeError('down')
        with patch(GLOBAL_LIMITER_LOGGER) as log:
            result = grl.expected_time(1)
        assert result == 0.0             # fresh bucket -> (max(1,cap)-cap)/rate == 0
        assert log.warning.called

    def test_fallback_warning_contains_no_url(self):
        grl, client = _make_limiter()
        client.register_script.return_value.side_effect = ConnectionError('boom')
        with patch(GLOBAL_LIMITER_LOGGER) as log:
            grl.can_consume(1)
        args = log.warning.call_args.args
        message = args[0] % tuple(args[1:])
        assert 'redis://' not in message
        assert 'rediss://' not in message
        assert 'celery:rate:app.add' in message

    def test_inherited_protocol_does_not_touch_redis(self):
        grl, client = _make_limiter()
        grl.add(('req', 1))
        grl.contents.appendleft(('req2', 1))
        assert grl.pop() == ('req2', 1)
        grl.clear_pending()
        assert len(grl.contents) == 0
        assert grl.fill_rate == 5.0
        assert grl.capacity == 5.0
        client.register_script.return_value.assert_not_called()

    def test_capacity_defaults_when_none(self):
        client = MagicMock()
        assert GlobalRateLimiter(2.0, client, 'x').capacity == 2.0
        assert GlobalRateLimiter(0.5, client, 'y').capacity == 1.0

    def test_repr_includes_key(self):
        grl, _ = _make_limiter(task_name='app.mul')
        assert 'celery:rate:app.mul' in repr(grl)


class test_get_rate_limiter_for_task:

    def test_returns_none_when_no_rate_limit(self):
        app = _make_app()
        assert get_rate_limiter_for_task(app, _make_task(rate_limit=None)) is None

    @pytest.mark.parametrize('rate_limit', ['0/s', '0/m', '0/h'])
    def test_returns_none_when_rate_parses_to_zero(self, rate_limit):
        app = _make_app()
        assert get_rate_limiter_for_task(app, _make_task(rate_limit=rate_limit)) is None

    def test_returns_tokenbucket_when_global_disabled(self):
        app = _make_app(enabled=False)
        result = get_rate_limiter_for_task(app, _make_task(rate_limit='5/s'))
        assert isinstance(result, TokenBucket)
        assert not isinstance(result, GlobalRateLimiter)

    def test_returns_tokenbucket_when_no_redis_anywhere(self):
        app = _make_app(enabled=True, broker_url='pyamqp://', backend=object())
        fake = _fake_redis_backend_module()
        with patch.dict(sys.modules, {'celery.backends.redis': fake}):
            result = get_rate_limiter_for_task(app, _make_task(rate_limit='5/s'))
        assert isinstance(result, TokenBucket)
        assert not isinstance(result, GlobalRateLimiter)

    def test_returns_global_limiter_for_redis_backend(self):
        fake = _fake_redis_backend_module()
        backend = fake.RedisBackend()
        backend.client = MagicMock(name='backend_client')
        app = _make_app(enabled=True, backend=backend)
        with patch.dict(sys.modules, {'celery.backends.redis': fake}):
            result = get_rate_limiter_for_task(app, _make_task(rate_limit='5/s'))
        assert isinstance(result, GlobalRateLimiter)
        assert result._redis is backend.client

    def test_returns_global_limiter_for_redis_broker(self):
        fake = _fake_redis_backend_module()
        app = _make_app(enabled=True, broker_url='redis://localhost/0', backend=object())
        conn = MagicMock(name='connection')
        app.connection_for_read.return_value = conn
        with patch.dict(sys.modules, {'celery.backends.redis': fake}):
            result = get_rate_limiter_for_task(app, _make_task(rate_limit='5/s'))
        assert isinstance(result, GlobalRateLimiter)
        assert result._redis is conn.default_channel.client

    def test_returns_tokenbucket_when_redis_construction_fails(self):
        # If GlobalRateLimiter construction raises (e.g. register_script errors),
        # the factory catches it and degrades to the per-process TokenBucket so
        # bucket_for_task never propagates an exception (R5 failure isolation).
        fake = _fake_redis_backend_module()
        backend = fake.RedisBackend()
        client = MagicMock(name='backend_client')
        client.register_script.side_effect = RuntimeError('bad client')
        backend.client = client
        app = _make_app(enabled=True, backend=backend)
        with patch.dict(sys.modules, {'celery.backends.redis': fake}):
            with patch('celery.utils.rate_limit.factory.logger'):
                result = get_rate_limiter_for_task(app, _make_task(rate_limit='5/s'))
        assert isinstance(result, TokenBucket)
        assert not isinstance(result, GlobalRateLimiter)
