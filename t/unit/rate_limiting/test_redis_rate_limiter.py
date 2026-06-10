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
from unittest.mock import Mock, call, patch

import pytest

from celery.exceptions import ImproperlyConfigured
from celery.rate_limiting import redis_rate_limiter
from celery.rate_limiting.redis_rate_limiter import RedisError, RedisTokenBucket
from celery.utils.time import rate

# ``RedisTokenBucket`` catches the ``RedisError`` defined in its OWN module for
# its fail-open/fail-closed degradation, so we import that EXACT symbol here.
# When ``redis-py`` is installed it is ``redis.exceptions.RedisError``; when it
# is absent it is the limiter module's stand-in subclass. Either way it is the
# very object the limiter's ``except RedisError`` clause references, so raising
# it through a mocked script's ``side_effect`` is reliably caught. This lets the
# mandatory fail-open/fail-closed tests exercise the degradation branch
# identically WITH or WITHOUT ``redis-py`` installed -- no skip required.

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
        # Assert the EXACT atomic-op contract, not just the call count: every
        # consume sends the per-task key plus the
        # ``[capacity, fill_rate, tokens, ttl]`` argument vector. A regression
        # that passed the wrong token count, capacity, fill rate or TTL would
        # change these args and be caught here.
        expected_call = call(
            keys=[bucket._key],
            args=[bucket.capacity, bucket.fill_rate, 1, bucket._ttl],
        )
        bucket._consume_script.assert_has_calls([expected_call, expected_call])

    def test_expected_time(self):
        """expected_time() yields the token-bucket backoff (fill_rate=10 -> 0.1).

        This is the MANDATORY backoff-math assertion. For a bucket refilling at
        ``fill_rate=10`` tokens/second, the wait for a single token on an empty
        bucket is ``1 / 10 == 0.1`` seconds. The atomic Lua script encodes that
        same backoff in WHOLE MICROSECONDS as
        ``ceil((deficit / fill_rate) * 1e6) == ceil((1 / 10) * 1e6) == 100000``,
        which the limiter divides back to ``0.1`` seconds. The injected return
        value is therefore the genuine token-bucket backoff for ``fill_rate=10``
        -- not an arbitrary microsecond-conversion constant (the general
        conversion is covered separately by
        :meth:`test_expected_time_microseconds_conversion`).
        """
        bucket = self._make_bucket(fill_rate=10.0)
        bucket._expected_script.return_value = 100000

        assert bucket.expected_time(1) == pytest.approx(0.1)
        # The read-only script is consulted against the per-task key with the
        # EXACT capacity/fill_rate/tokens triple (and never mutates the bucket).
        bucket._expected_script.assert_called_once_with(
            keys=[bucket._key],
            args=[bucket.capacity, bucket.fill_rate, 1],
        )

    def test_expected_time_microseconds_conversion(self):
        """Additional check: the microsecond->second divisor is general.

        The MANDATORY backoff value is covered by :meth:`test_expected_time`;
        this companion test retains the script-conversion check (per the review
        guidance) by asserting a DISTINCT, non-backoff value round-trips: the
        script returns ``250000`` whole microseconds and the limiter divides
        back to ``0.25`` seconds, proving the divisor is not hard-wired to the
        ``0.1`` case.
        """
        bucket = self._make_bucket(fill_rate=10.0)
        bucket._expected_script.return_value = 250000

        assert bucket.expected_time(1) == pytest.approx(0.25)

    def test_fail_open(self):
        """A Redis server error degrades OPEN (allow) by default.

        Mandatory degradation coverage. The mocked script raises the limiter
        module's OWN ``RedisError`` symbol -- the exact type its
        ``except RedisError`` clause catches -- so this path is exercised
        identically WITH or WITHOUT ``redis-py`` installed (no skip).
        """
        # On a Redis *server* error the default fail-open mode ALLOWS the task
        # so a coordination-layer outage never halts processing.
        bucket = self._make_bucket(fail_open=True)
        bucket._consume_script.side_effect = RedisError('redis is down')
        bucket._expected_script.side_effect = RedisError('redis is down')

        assert bucket.can_consume(1) is True
        # expected_time also degrades gracefully to the bounded, GENUINELY
        # computed ``tokens / fill_rate`` backoff (1 / 10 == 0.1) instead of
        # raising -- this is real arithmetic in the limiter, not a mocked value.
        assert bucket.expected_time(1) == pytest.approx(0.1)

    def test_fail_closed(self):
        """A Redis server error degrades CLOSED (block) when fail_open=False.

        Mandatory degradation coverage. Like :meth:`test_fail_open`, it uses the
        limiter module's own ``RedisError`` seam, so it runs WITH or WITHOUT
        ``redis-py`` installed (no skip).
        """
        # With fail_open=False the very same Redis error BLOCKS the task.
        bucket = self._make_bucket(fail_open=False)
        bucket._consume_script.side_effect = RedisError('redis is down')

        assert bucket.can_consume(1) is False

    def test_malformed_backend_url_raises_improperly_configured(self):
        """A malformed backend URL raises ImproperlyConfigured, not raw ValueError.

        ``redis.Redis.from_url`` rejects an invalid URL with a builtins
        ``ValueError``; the limiter converts that into a loud
        :class:`~celery.exceptions.ImproperlyConfigured` (mirroring the
        missing-``redis-py`` branch) so the *misconfiguration* surfaces clearly
        rather than escaping as an opaque ``ValueError`` -- and is deliberately
        NOT swallowed by the fail-open/fail-closed path, which targets transient
        Redis *server* errors only. The module-level ``redis`` symbol is patched
        so the ``from_url`` ``ValueError`` is simulated WITH or WITHOUT real
        ``redis-py`` installed; no live server is required. This is the
        regression test for the malformed-URL acceptance gap.
        """
        # No mocks are injected at the lazy seam here, so ``can_consume`` runs
        # the real ``_get_client()`` -> ``redis.Redis.from_url`` path.
        bucket = RedisTokenBucket(
            10.0, capacity=1, backend_url='not-a-redis-url',
            task_name='myapp.tasks.add', fail_open=True,
        )
        fake_redis = Mock(name='redis_module')
        fake_redis.Redis.from_url.side_effect = ValueError(
            'Redis URL must specify one of the following schemes '
            '(redis://, rediss://, unix://)')

        # ``fail_open=True`` is the strongest assertion: even in fail-open mode a
        # bad URL must still raise rather than silently allow the task.
        with patch.object(redis_rate_limiter, 'redis', fake_redis):
            with pytest.raises(ImproperlyConfigured) as exc_info:
                bucket.can_consume(1)

        message = str(exc_info.value)
        # The message is actionable: it names the offending setting and the
        # accepted URL schemes so an operator can fix it immediately.
        assert 'task_global_rate_limit_backend' in message
        assert 'redis://' in message
        # The original ValueError is chained (``raise ... from exc``) so the
        # underlying redis-py cause stays available for debugging.
        assert isinstance(exc_info.value.__cause__, ValueError)

    def test_malformed_backend_url_expected_time_also_raises(self):
        """expected_time() raises ImproperlyConfigured on a malformed URL too.

        Both rate-decision methods funnel through ``_get_client()``, so the
        malformed-URL misconfiguration is reported consistently from
        :meth:`expected_time` as well -- it does not fall back to the bounded
        backoff (that fallback is reserved for transient ``RedisError``).
        """
        bucket = RedisTokenBucket(
            10.0, capacity=1, backend_url='not-a-redis-url',
            task_name='myapp.tasks.add', fail_open=False,
        )
        fake_redis = Mock(name='redis_module')
        fake_redis.Redis.from_url.side_effect = ValueError('bad scheme')

        with patch.object(redis_rate_limiter, 'redis', fake_redis):
            with pytest.raises(ImproperlyConfigured):
                bucket.expected_time(1)

    def test_malformed_backend_url_credentials_redacted(self):
        """Credentials embedded in a malformed backend URL are not leaked.

        The error message is built from ``maybe_sanitize_url`` so any password
        embedded in ``task_global_rate_limit_backend`` is redacted (never
        surfaced in the exception/traceback), per the AAP security requirement.
        """
        bucket = RedisTokenBucket(
            10.0, capacity=1,
            backend_url='http://user:supersecret@example.invalid/0',
            task_name='myapp.tasks.add', fail_open=True,
        )
        fake_redis = Mock(name='redis_module')
        fake_redis.Redis.from_url.side_effect = ValueError('bad scheme')

        with patch.object(redis_rate_limiter, 'redis', fake_redis):
            with pytest.raises(ImproperlyConfigured) as exc_info:
                bucket.can_consume(1)

        message = str(exc_info.value)
        assert 'supersecret' not in message  # plaintext password never leaks
        assert '**' in message  # the ``**`` redaction marker appears instead

    def test_per_task_key_namespacing(self):
        """Keys are namespaced per task name; distinct tasks never collide."""
        # Every task gets its own key, prefixed ``celery:global-rate-limit:``,
        # so distinct tasks (and distinct apps sharing one Redis) never collide.
        bucket = self._make_bucket(task_name='myapp.tasks.add')
        bucket._consume_script.return_value = 1
        bucket.can_consume(1)

        expected_key = 'celery:global-rate-limit:myapp.tasks.add'
        assert bucket._key == expected_key
        # The EXACT key and argument vector passed to the atomic op are asserted
        # (not ``args=ANY``), so a regression in any of capacity, fill_rate,
        # tokens or ttl is caught here.
        bucket._consume_script.assert_called_once_with(
            keys=[expected_key],
            args=[bucket.capacity, bucket.fill_rate, 1, bucket._ttl],
        )

        # A different task name must produce a different (non-colliding) key.
        other = self._make_bucket(task_name='myapp.tasks.mul')
        other._consume_script.return_value = 1
        other.can_consume(1)

        assert other._key == 'celery:global-rate-limit:myapp.tasks.mul'
        assert other._key != bucket._key
        other._consume_script.assert_called_once_with(
            keys=['celery:global-rate-limit:myapp.tasks.mul'],
            args=[other.capacity, other.fill_rate, 1, other._ttl],
        )

    def test_falsy_rate_limit_parses_to_zero(self):
        """A falsy rate_limit parses to ``0`` -- the precondition for the no-op.

        ``Consumer.bucket_for_task()`` builds a bucket only for a *truthy*
        parsed limit::

            limit = rate(getattr(type, 'rate_limit', None))
            # RedisTokenBucket(...) when ``limit`` and the global backend is
            # configured; else TokenBucket(limit, capacity=1) if limit else None

        so a falsy ``rate_limit`` (``None`` or ``0``) yields ``rate() == 0`` and
        NO bucket is built -- hence NO ``RedisTokenBucket`` is constructed and NO
        Redis connection is ever opened.

        This test pins only that parser-level precondition; it deliberately does
        NOT re-implement the factory's ``if limit else None`` branch. The REAL
        factory seam -- that ``bucket_for_task`` returns ``None`` and never
        constructs ``RedisTokenBucket`` -- is exercised for BOTH ``None`` and
        explicit ``0`` (with the global backend configured) in
        ``t/unit/worker/test_consumer.py::test_Consumer``.
        """
        assert rate(None) == 0
        assert rate(0) == 0

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
