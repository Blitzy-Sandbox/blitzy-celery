"""Worker-hook tests for the global rate limiter's dispatch-strategy integration.

These tests exercise :func:`celery.worker.strategy.default`'s global rate-limit
gate end-to-end against a *controllable* fake limiter, proving the behaviours
the limiter's own unit tests (``test_redis_rate_limiter.py``) cannot: how the
**worker strategy** reacts to each of the limiter contract's three outcomes.

The final delivery-gate review flagged a CRITICAL regression here -- the
strategy cleared the per-process bucket for *every* allowed result, so a Redis
outage (previously reported as a grant) bypassed all rate limiting instead of
falling back to Celery's existing per-process limiter.  The limiter now raises
:class:`~celery.rate_limiting.base.RateLimiterUnavailable` to signal an
unavailable backend, and the strategy distinguishes it from a real grant.  The
tests below lock that contract in at the worker hook:

* **BACKEND UNAVAILABLE** (``can_consume`` raises ``RateLimiterUnavailable``,
  or any unexpected error) -> the worker FALLS BACK to the per-process limiter
  (``consumer._limit_task(req, bucket, 1)``); the task is NOT dispatched
  directly, so rate limiting is never silently removed.
* **CONFIRMED GRANT** (``(True, 0.0)``) -> the per-process bucket is bypassed
  (cleared) and the task dispatches directly, so the consumed cluster-wide
  token is not double-counted.
* **CONFIRMED DENIAL** (``(False, retry_after)``) -> the task is re-queued WITH
  DELAY via the consumer timer and RE-CHECKED on wake; it is never dropped.

The harness mirrors ``t/unit/worker/test_strategy.py`` and reuses the autouse
fixtures (``self.app``, ``self.task_message_from_sig``, ``self.TaskMessage``)
that ``t/unit/conftest.py`` injects into every test instance.  This file is
strictly additive -- it does not import from or modify any existing test.
"""
from unittest.mock import Mock

from kombu.utils.limits import TokenBucket

from celery.rate_limiting.base import RateLimiterUnavailable
from celery.utils.time import rate


class test_global_rate_limit_strategy:
    """``strategy.default()`` global-limiter gate: fallback, grant, deny, no-op."""

    #: Rate the gated task declares; also used to size the per-process bucket.
    RATE = '10/s'

    def setup_method(self):
        # A task that DECLARES a rate_limit so the global gate engages -- the
        # gate is skipped entirely for tasks without a rate_limit.
        @self.app.task(shared=False, rate_limit=self.RATE)
        def grl_task(x, y):
            return x + y

        self.grl_task = grl_task

    def _build(self, sig, limiter, rate_limits=True, with_bucket=True):
        """Wire a mock consumer + strategy for ``sig``; return the handles.

        Mirrors ``t/unit/worker/test_strategy.py::_context``: a ``Mock``
        consumer holding a real :class:`~kombu.utils.limits.TokenBucket` as the
        per-process bucket, the controllable ``limiter`` set explicitly as
        ``consumer.global_rate_limiter`` (a ``Mock`` would otherwise auto-create
        a truthy attribute), and a ``Mock`` ``task_reserved``.

        Returns:
            tuple: ``(handler, reserved, consumer, bucket)``.
        """
        reserved = Mock(name='task_reserved')
        consumer = Mock(name='consumer')
        bucket = TokenBucket(rate(self.RATE), capacity=1) if with_bucket else None
        task_buckets = Mock(name='task_buckets')
        task_buckets.__getitem__ = Mock(
            side_effect=lambda key: bucket if key == sig.task else None)
        consumer.task_buckets = task_buckets
        consumer.controller.state.revoked = set()
        consumer.disable_rate_limits = not rate_limits
        consumer.event_dispatcher.enabled = True
        # ``consumer`` is a Mock, so ``getattr(consumer, 'global_rate_limiter',
        # None)`` would return a truthy auto-Mock -- set it EXPLICITLY so the
        # strategy uses exactly the limiter (or None) this test intends.
        consumer.global_rate_limiter = limiter
        handler = sig.type.start_strategy(
            self.app, consumer, task_reserved=reserved)
        return handler, reserved, consumer, bucket

    def _deliver(self, handler, sig):
        """Feed a freshly-built task message through the strategy ``handler``."""
        message = self.task_message_from_sig(
            self.app, sig, TaskMessage=self.TaskMessage)
        handler(
            message,
            message.payload if not message.headers.get('id') else None,
            message.ack, message.reject, [],
        )
        return message

    # -- BACKEND UNAVAILABLE: the CRITICAL graceful-fallback path -----------

    def test_backend_unavailable_falls_back_to_per_process(self):
        """``RateLimiterUnavailable`` -> per-process ``limit_task`` runs (no bypass)."""
        sig = self.grl_task.s(2, 2)
        limiter = Mock(name='limiter')
        limiter.can_consume.side_effect = RateLimiterUnavailable('redis down')
        handler, reserved, consumer, bucket = self._build(sig, limiter)
        self._deliver(handler, sig)
        # The limiter was consulted with the task name and its declared rate...
        limiter.can_consume.assert_called_once_with(sig.task, self.RATE)
        # ...it raised, so the EXISTING per-process limiter handled the task
        # with the SAME bucket and a single token request...
        assert consumer._limit_task.call_count == 1
        called_req, called_bucket, called_n = consumer._limit_task.call_args[0]
        assert called_bucket is bucket
        assert called_n == 1
        # ...and the task was NOT dispatched directly: rate limiting is intact.
        assert not reserved.called

    def test_unexpected_limiter_error_also_falls_back(self):
        """Any unexpected limiter error also degrades to per-process limiting."""
        sig = self.grl_task.s(2, 2)
        limiter = Mock(name='limiter')
        # Not a RateLimiterUnavailable -- the strategy's defensive ``except``
        # must still fall back (warn + leave the bucket intact), never crash.
        limiter.can_consume.side_effect = RuntimeError('unexpected boom')
        handler, reserved, consumer, bucket = self._build(sig, limiter)
        self._deliver(handler, sig)
        assert consumer._limit_task.call_count == 1
        assert consumer._limit_task.call_args[0][1] is bucket
        assert not reserved.called

    # -- CONFIRMED GRANT: the only case that may bypass the bucket ----------

    def test_confirmed_grant_bypasses_per_process_bucket(self):
        """A real Redis grant dispatches directly, bypassing the bucket."""
        sig = self.grl_task.s(2, 2)
        limiter = Mock(name='limiter')
        limiter.can_consume.return_value = (True, 0.0)
        handler, reserved, consumer, bucket = self._build(sig, limiter)
        self._deliver(handler, sig)
        # Direct dispatch: a cluster-wide token was consumed in Redis, so the
        # per-process bucket is bypassed to avoid double-counting.
        assert reserved.called
        assert not consumer._limit_task.called
        consumer.on_task_request.assert_called()

    # -- CONFIRMED DENIAL: re-queue with delay, never drop ------------------

    def test_confirmed_denial_reschedules_without_drop(self):
        """A denial re-queues WITH DELAY via the timer and never drops the task."""
        sig = self.grl_task.s(2, 2)
        limiter = Mock(name='limiter')
        limiter.can_consume.return_value = (False, 2.5)
        handler, reserved, consumer, bucket = self._build(sig, limiter)
        self._deliver(handler, sig)
        # Neither dispatched nor per-process rate-limited -> deferred, not lost.
        assert not reserved.called
        assert not consumer._limit_task.called
        # Re-queued WITH DELAY on the consumer timer, using the reported wait.
        consumer.timer.call_after.assert_called_once()
        delay = consumer.timer.call_after.call_args[0][0]
        assert delay == 2.5
        # The qos slot is incremented (balanced on eventual dispatch).
        consumer.qos.increment_eventually.assert_called_once_with()

    def test_denied_then_rechecked_dispatches_on_later_grant(self):
        """The rescheduled callback RE-CHECKS the limiter and dispatches on grant."""
        sig = self.grl_task.s(2, 2)
        limiter = Mock(name='limiter')
        # First check (at dispatch) denies; second check (on wake) grants.
        limiter.can_consume.side_effect = [(False, 1.0), (True, 0.0)]
        handler, reserved, consumer, bucket = self._build(sig, limiter)
        self._deliver(handler, sig)
        # Recover the scheduled recheck callback + request and fire it manually.
        callback = consumer.timer.call_after.call_args[0][1]
        req = consumer.timer.call_after.call_args[0][2][0]
        callback(req)
        # On the second (granting) check the task dispatches via apply_eta_task
        # (which balances the qos slot incremented when it was first delayed).
        consumer.apply_eta_task.assert_called_once_with(req)
        assert limiter.can_consume.call_count == 2

    def test_recheck_backend_unavailable_dispatches_no_drop(self):
        """If Redis dies during a recheck, the admitted task dispatches (no drop)."""
        sig = self.grl_task.s(2, 2)
        limiter = Mock(name='limiter')
        # Denied first, then the backend goes unavailable on the recheck.
        limiter.can_consume.side_effect = [
            (False, 1.0), RateLimiterUnavailable('redis down')]
        handler, reserved, consumer, bucket = self._build(sig, limiter)
        self._deliver(handler, sig)
        callback = consumer.timer.call_after.call_args[0][1]
        req = consumer.timer.call_after.call_args[0][2][0]
        callback(req)
        # The already-admitted task is dispatched rather than re-queued forever
        # while Redis is down -- never dropped.
        consumer.apply_eta_task.assert_called_once_with(req)

    # -- the gate must stay OUT of the way when it should -------------------

    def test_task_without_rate_limit_is_unaffected(self):
        """A task that declares no rate_limit never consults the global limiter."""

        @self.app.task(shared=False)  # no rate_limit -> gate is skipped
        def plain_task(x, y):
            return x + y

        sig = plain_task.s(2, 2)
        limiter = Mock(name='limiter')
        # No per-process bucket either, mirroring reality (no rate_limit means
        # the consumer builds no bucket for the task).
        handler, reserved, consumer, bucket = self._build(
            sig, limiter, with_bucket=False)
        self._deliver(handler, sig)
        limiter.can_consume.assert_not_called()
        assert reserved.called  # plain direct dispatch, unchanged behaviour

    def test_disabled_global_limiter_uses_per_process(self):
        """With the limiter disabled (None) the per-process bucket path runs."""
        sig = self.grl_task.s(2, 2)
        # Explicit None -> the strategy asks the factory, which returns None for
        # the default (disabled) app config, so the global gate is skipped and
        # behaviour is the unchanged per-process path.
        handler, reserved, consumer, bucket = self._build(sig, None)
        self._deliver(handler, sig)
        assert consumer._limit_task.call_count == 1
        assert consumer._limit_task.call_args[0][1] is bucket
        assert not reserved.called
