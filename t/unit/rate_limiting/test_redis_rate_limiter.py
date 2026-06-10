"""Unit tests for the global (cluster-wide) Redis rate limiter.

Covers the new, opt-in, default-OFF rate-limiting package
:mod:`celery.rate_limiting`:

* :class:`celery.rate_limiting.base.BaseRateLimiter` -- the abstract contract.
* :class:`celery.rate_limiting.redis_rate_limiter.RedisRateLimiter` -- the
  concrete Redis token-bucket backend (atomic Lua script, graceful fallback).
* :func:`celery.rate_limiting.get_global_rate_limiter` -- the factory that
  returns a configured limiter when the feature is enabled, otherwise ``None``.

The token-bucket Lua script is executed *for real* against ``fakeredis`` (the
``[lua]`` extra is required), so the aggregate-cap, refill, atomicity and TTL
behaviours are validated end-to-end without a live Redis server.  The
missing-library and connection-error fallback paths are driven with
:mod:`unittest.mock`.  The whole module skips cleanly via
:func:`pytest.importorskip` when ``fakeredis``/``redis`` is unavailable.

These tests are strictly additive and self-contained: they do not import from
or modify the shared ``t/unit/conftest.py`` (only the existing ``app`` fixture
it already provides is consumed).
"""
import logging
import threading
from contextlib import contextmanager
from unittest.mock import ANY, Mock, call, patch

import pytest

# Guard the optional test dependencies so the suite skips cleanly when absent.
# ``fakeredis`` needs its ``[lua]`` extra to execute the limiter's Lua script;
# ``redis`` (redis-py) supplies the exception classes the limiter catches.
fakeredis = pytest.importorskip('fakeredis')
pytest.importorskip('redis')
# The limiter is implemented as a Redis Lua script and these tests EXECUTE that
# Lua under ``fakeredis``, which requires fakeredis' optional ``[lua]`` extra
# (the ``lupa`` package).  A plain ``fakeredis`` install WITHOUT Lua support
# would error at EVAL time instead of skipping, so guard explicitly on ``lupa``
# AND smoke-test a trivial ``EVAL`` -- whichever check trips, the whole module
# SKIPS cleanly (never ERRORs), as the checkpoint requires.
pytest.importorskip(
    'lupa',
    reason="fakeredis[lua] (the 'lupa' package) is required to execute the "
           "limiter's Lua token-bucket script",
    # Skip (not error) on ANY ImportError -- e.g. a present-but-broken ``lupa``
    # -- and stay forward-compatible with pytest 9.1's stricter default.
    exc_type=ImportError,
)
try:
    if fakeredis.FakeStrictRedis().eval('return 1', 0) != 1:
        raise RuntimeError('unexpected EVAL result')
except Exception as exc:  # pragma: no cover - environment-dependent skip
    pytest.skip(
        f'fakeredis cannot execute Lua scripts ({exc!r}); install '
        'fakeredis[lua] to run the global rate-limiter unit tests',
        allow_module_level=True,
    )

try:
    from redis import exceptions as redis_exceptions
except ImportError:  # pragma: no cover
    redis_exceptions = None

from kombu.utils.url import maybe_sanitize_url  # noqa: E402

import celery.rate_limiting.redis_rate_limiter as rrl_module  # noqa: E402
from celery.rate_limiting import (BaseRateLimiter, RateLimiterUnavailable, RedisRateLimiter, base,  # noqa: E402
                                  get_global_rate_limiter)
from celery.utils.time import rate as rate_to_tps  # noqa: E402

#: Runtime logger name of the limiter module (``get_logger(__name__)``); used to
#: scope ``caplog`` capture and filter the single sanitized fallback warning.
LOGGER_NAME = 'celery.rate_limiting.redis_rate_limiter'

#: Plain backend URL (no credentials) used by the happy-path behavioural tests.
URL = 'redis://localhost:6379/0'

#: Backend URL embedding a password, used to assert the fallback warning logs a
#: *sanitized* URL (``maybe_sanitize_url`` masks the secret) -- never the raw
#: credentials.  The password value is an obvious fake.
URL_WITH_SECRET = 'redis://:secret@localhost:6379/0'

#: Per-task bucket key prefix, mirrored from the limiter module's contract so
#: the TTL / key-inspection tests can address the exact bucket key.
KEY_PREFIX = 'celery:global_rate_limit:'


@contextmanager
def shared_fake_redis(server):
    """Patch ``rrl_module.redis`` so the limiter binds to one fakeredis keyspace.

    Replaces the limiter module's guarded ``redis`` attribute with a stand-in
    whose ``ConnectionPool.from_url(...)`` returns a sentinel pool and whose
    ``Redis(connection_pool=...)`` returns a real
    :class:`fakeredis.FakeStrictRedis` bound to ``server``.  Because every
    client shares the same :class:`fakeredis.FakeServer`, multiple
    :class:`RedisRateLimiter` instances (each simulating a separate worker)
    coordinate through a single shared keyspace -- and the limiter's real
    ``_get_client``/``register_script``/EVAL code path is exercised end to end.

    Arguments:
        server (fakeredis.FakeServer): The shared in-memory server every client
            built within the context attaches to.

    Yields:
        unittest.mock.Mock: The stand-in ``redis`` module, so callers may make
        additional assertions on it if needed.
    """
    fake_redis_attr = Mock(name='redis_module')
    fake_redis_attr.ConnectionPool.from_url.return_value = Mock(name='pool')

    def _make_client(connection_pool=None, **kwargs):
        # Ignore the sentinel pool; every client shares ``server`` so they all
        # read and write the same keyspace, as real workers would in a cluster.
        return fakeredis.FakeStrictRedis(server=server)

    fake_redis_attr.Redis.side_effect = _make_client
    with patch.object(rrl_module, 'redis', fake_redis_attr):
        yield fake_redis_attr


@contextmanager
def frozen_time(value=1000.0):
    """Patch ``rrl_module.time`` so the limiter's wall clock is deterministic.

    The limiter reads ``time.time()`` through its module-level ``time`` import
    to stamp each token-bucket refill.  Patching only ``rrl_module.time`` (not
    the global :mod:`time`) freezes the limiter's clock while leaving
    ``fakeredis``'s own time handling untouched, so token refill is fully
    deterministic: with the clock frozen no time elapses between calls and the
    bucket never refills, making the aggregate ceiling exact.

    Arguments:
        value (float): The initial frozen wall-clock value in seconds.

    Yields:
        unittest.mock.Mock: The stand-in ``time`` module.  Mutate
        ``fake_time.time.return_value`` to advance the clock (used by the
        refill test to prove time-based token replenishment without sleeping).
    """
    fake_time = Mock(name='time_module')
    fake_time.time.return_value = value
    with patch.object(rrl_module, 'time', fake_time):
        yield fake_time


class test_BaseRateLimiter:
    """Contract tests for the backend-agnostic abstract base class."""

    def test_not_instantiable(self):
        """The abstract base cannot be instantiated directly."""
        # ``BaseRateLimiter`` declares an abstract ``can_consume`` via
        # ``abc.ABCMeta``; instantiating it must raise ``TypeError``.
        with pytest.raises(TypeError):
            BaseRateLimiter()

    def test_concrete_subclass_instantiable(self):
        """A subclass implementing ``can_consume`` is instantiable and works."""

        class _Concrete(BaseRateLimiter):
            def can_consume(self, task_name, rate):
                return (True, 0.0)

        limiter = _Concrete()
        assert isinstance(limiter, BaseRateLimiter)
        # The overridden method is callable and honours the (allowed,
        # retry_after) return contract.
        assert limiter.can_consume('some.task', '10/s') == (True, 0.0)

    def test_all_exports(self):
        """``base.__all__`` exposes the base class and the fallback signal."""
        # ``RateLimiterUnavailable`` is part of the limiter contract: it is the
        # dedicated exception a backend raises to request per-process fallback.
        assert base.__all__ == ('BaseRateLimiter', 'RateLimiterUnavailable')


class test_RedisRateLimiter:
    """Behavioural and resilience tests for the Redis token-bucket limiter."""

    def test_aggregate_cap_across_workers(self):
        """The configured rate is a cluster-wide ceiling, not per-instance."""
        # capacity = max(1.0, tps); for "5/s" that is 5.0 tokens.
        assert max(1.0, rate_to_tps('5/s')) == 5.0
        server = fakeredis.FakeServer()
        allowed = 0
        allowed_retries = []
        denied_retries = []
        # Three separate limiter instances simulate three worker processes, all
        # coordinating through the single shared fakeredis keyspace.  The clock
        # is frozen so no refill occurs and the ceiling is exact.
        with shared_fake_redis(server), frozen_time():
            limiters = [RedisRateLimiter(backend_url=URL) for _ in range(3)]
            for i in range(30):
                ok, retry = limiters[i % 3].can_consume('agg_task', '5/s')
                if ok:
                    allowed += 1
                    allowed_retries.append(retry)
                else:
                    denied_retries.append(retry)
        # Exactly the capacity is granted across ALL instances combined...
        assert allowed == 5
        # ...proving the limit is cluster-wide, NOT 5 per instance (3 * 5 = 15).
        assert allowed < 3 * 5
        # Allowed calls never ask the caller to wait; denied calls always do.
        assert all(retry == 0.0 for retry in allowed_retries)
        assert denied_retries  # there must have been denials
        assert all(retry > 0 for retry in denied_retries)

    def test_missing_library_fallback_warns_once(self, caplog, monkeypatch):
        """A missing redis-py library signals fallback with exactly one warning."""
        # Route the limiter's logger to caplog's root handler, but via
        # ``monkeypatch`` so the original ``propagate`` value is RESTORED after
        # the test -- no logging configuration leaks into later tests.
        monkeypatch.setattr(logging.getLogger(LOGGER_NAME), 'propagate', True)
        # With ``redis is None`` the client cannot be built, so the limiter
        # warns once and RAISES RateLimiterUnavailable (a missing backend is NOT
        # a grant): the worker must fall back to the per-process limiter.
        with patch.object(rrl_module, 'redis', None):
            limiter = RedisRateLimiter(backend_url=URL)
            with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
                with pytest.raises(RateLimiterUnavailable):
                    limiter.can_consume('t', '10/s')
                # A second call on the SAME instance also raises but must NOT
                # warn again (one warning per instance -- no log flooding).
                with pytest.raises(RateLimiterUnavailable):
                    limiter.can_consume('t', '10/s')
        records = [r for r in caplog.records
                   if r.name == LOGGER_NAME and r.levelname == 'WARNING']
        assert len(records) == 1

    def test_connection_error_fallback_warns_once(self, caplog, monkeypatch):
        """A Redis connection error during EVAL signals fallback with one warning."""
        monkeypatch.setattr(logging.getLogger(LOGGER_NAME), 'propagate', True)

        # --- Scenario 1: a realistically disconnected fakeredis server.  The
        # registered script raises a genuine redis.exceptions.ConnectionError
        # when the Lua EVAL is attempted, exercising can_consume's error path:
        # it warns once and RAISES RateLimiterUnavailable to request fallback.
        server = fakeredis.FakeServer()
        server.connected = False
        with shared_fake_redis(server):
            limiter = RedisRateLimiter(backend_url=URL)
            with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
                with pytest.raises(RateLimiterUnavailable):
                    limiter.can_consume('t', '10/s')
                # A second call on the SAME instance also raises but must NOT
                # warn again (one warning per instance -- no log flooding).
                with pytest.raises(RateLimiterUnavailable):
                    limiter.can_consume('t', '10/s')
        records = [r for r in caplog.records
                   if r.name == LOGGER_NAME and r.levelname == 'WARNING']
        assert len(records) == 1

        # --- Scenario 2: a stand-in client whose registered script raises a
        # ConnectionError, letting us assert the exact atomic EVAL invocation
        # (the per-task key plus the four positional script arguments).
        caplog.clear()
        failing_script = Mock(
            name='script', side_effect=redis_exceptions.ConnectionError('boom'))
        fake_redis_attr = Mock(name='redis_module')
        fake_redis_attr.ConnectionPool.from_url.return_value = Mock(name='pool')
        fake_client = Mock(name='client')
        fake_client.register_script.return_value = failing_script
        fake_redis_attr.Redis.return_value = fake_client
        with patch.object(rrl_module, 'redis', fake_redis_attr):
            limiter2 = RedisRateLimiter(backend_url=URL)
            with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
                with pytest.raises(RateLimiterUnavailable):
                    limiter2.can_consume('t', '10/s')
        # The consume is a single registered-script EVAL keyed per task; ``ANY``
        # matches the [tps, capacity, now, requested] argument vector.  This is a
        # public-seam assertion (patching the module's ``redis`` attribute), not
        # a private-internals one.
        failing_script.assert_called_once_with(
            keys=[f'{KEY_PREFIX}t'], args=ANY)
        assert failing_script.call_args == call(keys=[f'{KEY_PREFIX}t'], args=ANY)
        records2 = [r for r in caplog.records
                    if r.name == LOGGER_NAME and r.levelname == 'WARNING']
        assert len(records2) == 1

    def test_fallback_warning_sanitizes_url(self, caplog, monkeypatch):
        """The fallback warning logs a sanitized URL, never raw credentials."""
        monkeypatch.setattr(logging.getLogger(LOGGER_NAME), 'propagate', True)
        with patch.object(rrl_module, 'redis', None):
            limiter = RedisRateLimiter(backend_url=URL_WITH_SECRET)
            with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
                with pytest.raises(RateLimiterUnavailable):
                    limiter.can_consume('t', '10/s')
        # Assert ONLY on publicly-observable output (the captured log text), not
        # on any private attribute: the embedded password must never appear,
        # while the sanitized URL (host preserved, secret masked by the same
        # ``maybe_sanitize_url`` helper the limiter uses) must.
        assert 'secret' not in caplog.text
        assert 'localhost' in caplog.text
        assert maybe_sanitize_url(URL_WITH_SECRET) in caplog.text

    def test_no_rate_limit_is_noop(self):
        """An unset/zero rate is a pure no-op that never touches Redis."""
        # ``rate`` yields 0 for each of these, so the limiter must short-circuit
        # to (True, 0.0) WITHOUT ever building a client / hitting Redis.
        assert rate_to_tps(None) == 0
        assert rate_to_tps(0) == 0
        assert rate_to_tps('0/s') == 0
        # Patch the PUBLIC seam (the module's ``redis`` attribute) with a fake
        # module and assert the no-op path never constructs a client through it
        # -- proving it returns before ANY backend interaction, without reaching
        # into the limiter's private ``_get_client``.
        fake_redis_attr = Mock(name='redis_module')
        with patch.object(rrl_module, 'redis', fake_redis_attr):
            limiter = RedisRateLimiter(backend_url=URL)
            for noop_rate in (None, 0, '0/s'):
                assert limiter.can_consume('t', noop_rate) == (True, 0.0)
            # No client/pool was ever built: the backend was never consulted.
            fake_redis_attr.ConnectionPool.from_url.assert_not_called()
            fake_redis_attr.Redis.assert_not_called()

        # Reinforcement: with a real shared keyspace, no-op rates write nothing.
        server = fakeredis.FakeServer()
        with shared_fake_redis(server):
            limiter2 = RedisRateLimiter(backend_url=URL)
            for noop_rate in (None, 0, '0/s'):
                assert limiter2.can_consume('noop_task', noop_rate) == (True, 0.0)
            inspector = fakeredis.FakeStrictRedis(server=server)
            assert inspector.keys('*') == []

    def test_rate_parser_locked_values(self):
        """Lock the EXACT tokens-per-second the parser yields for each format.

        ``test_rate_formats_enforced`` only checks integer burst counts, and
        ``"100/m"`` and ``"1000/h"`` both grant a single initial token -- so a
        regression that mis-parsed every rate as ``1/s`` would still pass it.
        Pin the precise fractional rates here so such drift is caught: the
        limiter consumes ``rate_to_tps(rate)`` tokens/sec, and these are the
        values it must receive.
        """
        assert rate_to_tps('10/s') == pytest.approx(10.0)
        assert rate_to_tps('100/m') == pytest.approx(100 / 60)    # ~1.6667/s
        assert rate_to_tps('1000/h') == pytest.approx(1000 / 3600)  # ~0.2778/s
        # Unset/zero rates parse to 0 (the no-op sentinel), NOT a positive rate.
        assert rate_to_tps(None) == 0
        assert rate_to_tps(0) == 0
        assert rate_to_tps('0/s') == 0
        # ``None`` and ``0`` must agree -- both are the no-op sentinel.
        assert rate_to_tps(None) == rate_to_tps(0)

    @pytest.mark.parametrize('rate_str, expected_allowed', [
        ('10/s', 10),
        ('100/m', 1),
        ('1000/h', 1),
    ])
    def test_rate_formats_enforced(self, rate_str, expected_allowed):
        """Each rate-string format parses and enforces the right ceiling.

        Initial burst = whole tokens consumable from a full bucket of
        ``capacity = max(1.0, tps)``: ``"10/s"`` -> 10, ``"100/m"`` -> 1 (cap
        ~1.6667), ``"1000/h"`` -> 1 (cap 1.0).
        """
        # Sanity-check the capacity derivation that produces ``expected_allowed``.
        capacity = max(1.0, rate_to_tps(rate_str))
        assert int(capacity) >= expected_allowed - 0  # capacity covers the burst
        server = fakeredis.FakeServer()
        allowed = 0
        first_denied = None
        with shared_fake_redis(server), frozen_time():
            limiter = RedisRateLimiter(backend_url=URL)
            for _ in range(expected_allowed + 3):
                ok, retry = limiter.can_consume('fmt_task', rate_str)
                if ok:
                    allowed += 1
                elif first_denied is None:
                    first_denied = (ok, retry)
        assert allowed == expected_allowed
        # The first denial reports not-allowed and a positive wait time.
        assert first_denied is not None
        assert first_denied[0] is False
        assert first_denied[1] > 0

    def test_tokens_refill_over_time(self):
        """Tokens refill as the (frozen, then advanced) clock moves forward."""
        server = fakeredis.FakeServer()
        with shared_fake_redis(server), frozen_time(1000.0) as ft:
            limiter = RedisRateLimiter(backend_url=URL)
            # capacity = max(1.0, 2.0) = 2 -> two allowed, third denied.
            assert limiter.can_consume('refill_task', '2/s')[0] is True
            assert limiter.can_consume('refill_task', '2/s')[0] is True
            denied, retry = limiter.can_consume('refill_task', '2/s')
            assert denied is False
            assert retry > 0
            # Advance the limiter's clock by one second: +1s * 2/s = 2 tokens,
            # capped at capacity 2 -> two more calls succeed (no real sleep).
            ft.time.return_value = 1001.0
            assert limiter.can_consume('refill_task', '2/s')[0] is True
            assert limiter.can_consume('refill_task', '2/s')[0] is True
            exhausted, retry_again = limiter.can_consume('refill_task', '2/s')
            assert exhausted is False
            assert retry_again > 0

    def test_lua_atomicity_under_concurrency(self):
        """Concurrent EVALs cannot over-grant beyond capacity (atomic Lua)."""
        server = fakeredis.FakeServer()
        n_workers = 100
        allowed = []
        lock = threading.Lock()
        with shared_fake_redis(server), frozen_time():
            limiters = [RedisRateLimiter(backend_url=URL)
                        for _ in range(n_workers)]
            # Pre-build each limiter's client/script up front -- via a PUBLIC
            # warm-up consume on a SEPARATE key -- so the threaded section only
            # performs the atomic EVAL on ``atomic_task`` and avoids any mock
            # side-effect race when constructing the stand-in clients.  Using a
            # different task name leaves the ``atomic_task`` bucket pristine, and
            # no private internals are touched.
            for limiter in limiters:
                limiter.can_consume('atomic_warmup', '50/s')

            def worker(limiter):
                ok, _ = limiter.can_consume('atomic_task', '50/s')
                if ok:
                    with lock:
                        allowed.append(1)

            threads = [threading.Thread(target=worker, args=(limiter,))
                       for limiter in limiters]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        # capacity = max(1.0, 50.0) = 50; frozen time removes refill noise.  The
        # atomic check-and-consume guarantees no interleaving over-grants tokens.
        assert 1 <= len(allowed) <= 50

    def test_window_reset_ttl_set(self):
        """An allowed consume sets a TTL so the window resets (no stuck tokens)."""
        server = fakeredis.FakeServer()
        with shared_fake_redis(server):
            limiter = RedisRateLimiter(backend_url=URL)
            ok, retry = limiter.can_consume('ttl_task', '10/s')
            assert ok is True
            assert retry == 0.0
            inspector = fakeredis.FakeStrictRedis(server=server)
            key = f'{KEY_PREFIX}ttl_task'
            # The bucket key exists and carries a positive TTL, so a crashed
            # worker never permanently holds tokens and windows reset cleanly.
            assert inspector.exists(key)
            assert inspector.pttl(key) > 0

    def test_stale_timestamp_does_not_overgrant(self):
        """A stale/out-of-order timestamp must not regress the bucket and overgrant.

        Deterministic reproduction of the cluster-wide overgrant a *non*-monotonic
        bucket timestamp permits.  Independent workers stamp each ``can_consume``
        from their own wall clock, so a slow/late worker can deliver a timestamp
        EARLIER than one already recorded.  If the Lua script persisted that lower
        timestamp, a subsequent call would re-refill the SAME elapsed interval a
        second time and grant a token above the aggregate ceiling.  At ``"10/s"``
        (capacity 10) no more than 11 tokens may be granted by ``t0 + 0.1`` (10
        initial + exactly 1 refilled over 0.1s); the stale call must not yield a
        12th.

        This is the regression guard for the timestamp-regression overgrant: it
        FAILS against a script that stores ``now`` unconditionally (granting 12)
        and PASSES once the script clamps ``now`` up to ``last`` so the stored
        timestamp is monotonic.
        """
        server = fakeredis.FakeServer()
        granted = 0
        # The clock is driven explicitly so the out-of-order call is exact and
        # the refill window is a precise 0.1s -- no sleeping, fully deterministic.
        with shared_fake_redis(server), frozen_time(1000.0) as ft:
            limiter = RedisRateLimiter(backend_url=URL)
            # Drain the full bucket at t0 (capacity = max(1.0, 10.0) = 10).
            for _ in range(10):
                ok, _ = limiter.can_consume('stale_task', '10/s')
                granted += int(ok)
            assert granted == 10
            # 11th at t0 is denied -- the bucket is empty and no time elapsed.
            assert limiter.can_consume('stale_task', '10/s')[0] is False
            # Advance 0.1s: +0.1 * 10 = 1 token -> the 11th token is granted.
            ft.time.return_value = 1000.1
            ok, _ = limiter.can_consume('stale_task', '10/s')
            assert ok is True
            granted += 1
            # STALE/out-of-order call at t0 (< last = 1000.1): must be denied and,
            # with a monotonic timestamp, must NOT rewind the stored timestamp.
            ft.time.return_value = 1000.0
            assert limiter.can_consume('stale_task', '10/s')[0] is False
            # Back at t0 + 0.1: no NEW time has elapsed since the 1000.1 grant, so
            # a correct limiter denies (no 12th token).  The pre-fix bug re-filled
            # the same 0.1s interval here and granted a spurious 12th token.
            ft.time.return_value = 1000.1
            ok, _ = limiter.can_consume('stale_task', '10/s')
            granted += int(ok)
            assert ok is False
        # The aggregate ceiling holds: 10 initial capacity + exactly 1 refill.
        assert granted == 11

    def test_stale_timestamp_not_persisted_backwards(self):
        """The stored bucket timestamp is monotonic and never regresses.

        Directly asserts the monotonic-timestamp invariant underpinning the
        aggregate-ceiling guarantee: after a later call advances the bucket
        clock, an out-of-order call carrying an EARLIER timestamp must not rewind
        the persisted ``timestamp`` field -- otherwise a following call would
        re-refill an already-counted interval and overgrant.
        """
        server = fakeredis.FakeServer()
        key = f'{KEY_PREFIX}ts_monotonic_task'
        with shared_fake_redis(server), frozen_time(1000.0) as ft:
            limiter = RedisRateLimiter(backend_url=URL)
            limiter.can_consume('ts_monotonic_task', '10/s')   # stamps 1000.0
            ft.time.return_value = 1005.0
            limiter.can_consume('ts_monotonic_task', '10/s')   # advances to 1005.0
            ft.time.return_value = 1000.0                       # stale/out-of-order
            limiter.can_consume('ts_monotonic_task', '10/s')
            inspector = fakeredis.FakeStrictRedis(server=server)
            stored = inspector.hget(key, 'timestamp')
            stored_ts = float(
                stored.decode() if isinstance(stored, bytes) else stored)
        # The stale call must NOT have rewound the timestamp below 1005.0.
        assert stored_ts == pytest.approx(1005.0)

    def test_connection_pool_is_bounded(self):
        """The Redis connection pool is built with an explicit, bounded size.

        redis-py's ``ConnectionPool`` otherwise defaults ``max_connections`` to an
        effectively unbounded value (``2**31``); the limiter must instead pass an
        explicit, bounded ceiling so a burst of highly-concurrent dispatch cannot
        open an unbounded number of sockets, and that ceiling must be tunable.
        """
        server = fakeredis.FakeServer()
        with shared_fake_redis(server) as fake:
            limiter = RedisRateLimiter(backend_url=URL)
            limiter.can_consume('pool_bound_task', '10/s')  # forces pool build
            _, kwargs = fake.ConnectionPool.from_url.call_args
            assert kwargs['max_connections'] == rrl_module.DEFAULT_MAX_CONNECTIONS
            assert kwargs['max_connections'] < 2 ** 31
        # An explicit override flows straight through to pool construction.
        server2 = fakeredis.FakeServer()
        with shared_fake_redis(server2) as fake2:
            limiter2 = RedisRateLimiter(backend_url=URL, max_connections=5)
            limiter2.can_consume('pool_bound_task2', '10/s')
            _, kwargs2 = fake2.ConnectionPool.from_url.call_args
            assert kwargs2['max_connections'] == 5


class test_get_global_rate_limiter:
    """Factory wiring: default-off, opt-in, and never-raises behaviour."""

    def test_disabled_returns_none(self, app):
        """With the feature disabled (the default) the factory returns None."""
        # Default configuration: global_rate_limit_enabled is False.
        assert get_global_rate_limiter(app) is None

    def test_enabled_without_url_returns_none(self, app):
        """Enabled but with no backend URL stays a no-op (returns None)."""
        app.conf.global_rate_limit_enabled = True
        # Leave global_rate_limit_backend_url unset/None on purpose.
        assert get_global_rate_limiter(app) is None

    def test_enabled_with_url_returns_limiter(self, app):
        """Enabled with a backend URL yields a configured RedisRateLimiter."""
        app.conf.global_rate_limit_enabled = True
        app.conf.global_rate_limit_backend_url = URL
        result = get_global_rate_limiter(app)
        assert result is not None
        assert isinstance(result, RedisRateLimiter)

    def test_never_raises(self):
        """The factory swallows any error during wiring and returns None."""

        class _BoomApp:
            @property
            def conf(self):
                # Accessing configuration explodes; the factory must absorb it
                # and fall back to None rather than crashing worker startup.
                raise RuntimeError('boom')

        assert get_global_rate_limiter(_BoomApp()) is None
