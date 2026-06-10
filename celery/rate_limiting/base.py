"""Abstract base class for global (cluster-wide) rate limiters.

This module defines :class:`BaseRateLimiter`, the backend-agnostic contract
for Celery's optional, opt-in global (cluster-wide) rate limiter.  Concrete
backends -- for example the Redis token-bucket implementation in
:mod:`celery.rate_limiting.redis_rate_limiter` -- subclass this base and
provide the atomic :meth:`BaseRateLimiter.can_consume` decision.

The contract is consumed by the worker dispatch strategy
(:mod:`celery.worker.strategy`), which consults the configured limiter just
before dispatching a task that declares a ``rate_limit``.  Keeping the
contract here -- and dependency-free (standard library only) -- means the
worker hook never needs to know about Redis or any particular store, and the
module imports cleanly even on installations that do not have ``redis-py``
available.
"""
from __future__ import annotations

import abc

__all__ = ('BaseRateLimiter', 'RateLimiterUnavailable')


class RateLimiterUnavailable(Exception):
    """Signal that a limiter cannot make a cluster-wide decision right now.

    A concrete :class:`BaseRateLimiter` raises this from
    :meth:`BaseRateLimiter.can_consume` when its shared backend is
    unavailable -- for example the backend client library is not installed,
    the configured backend cannot be reached, or the atomic evaluation errors.
    It instructs the worker dispatch strategy (:mod:`celery.worker.strategy`)
    to **fall back to Celery's existing per-process rate limiting** for this
    task instead of treating the call as a granted cluster-wide token.

    Raising is deliberate and is the contract's third, distinct outcome:

    * an *enforced grant* returns ``(True, 0.0)``,
    * an *enforced denial* returns ``(False, retry_after)``, and
    * an *unavailable backend* raises :class:`RateLimiterUnavailable`.

    Distinguishing fallback from a real grant is essential to correctness.  If
    a backend failure were reported as ``(True, 0.0)`` it would be
    indistinguishable from a genuine grant, and the strategy -- which bypasses
    the per-process bucket on a grant so a consumed token is not double
    counted -- would bypass per-process limiting during a backend outage,
    silently removing *all* rate limiting.  Raising instead lets the strategy
    leave the per-process bucket intact and degrade gracefully.

    A no-op rate (an unset or zero ``rate_limit``) is **not** an unavailable
    backend: implementations treat it as always-allowed and return
    ``(True, 0.0)`` without raising.
    """


class BaseRateLimiter(metaclass=abc.ABCMeta):
    """Abstract base class defining the global rate-limiter contract.

    A :class:`BaseRateLimiter` is a backend-agnostic interface that decides,
    atomically and cluster-wide, whether a task may be dispatched right now
    given its declared rate.  Concrete subclasses implement
    :meth:`can_consume` against a specific shared store (for example Redis),
    so that a task's configured ``rate_limit`` becomes a true aggregate
    ceiling across every worker process in the cluster rather than an
    independent per-process limit.

    Subclasses **must** implement :meth:`can_consume`.  Because this class
    uses :class:`abc.ABCMeta` as its metaclass and declares an abstract
    method, it cannot be instantiated directly -- attempting to do so raises
    :exc:`TypeError`.
    """

    @abc.abstractmethod
    def can_consume(self, task_name: str,
                    rate: str | int | float | None) -> tuple[bool, float]:
        """Atomically decide whether a task may be dispatched now.

        Arguments:
            task_name (str): The fully-qualified task name (used to key the
                limiter's per-task bucket).
            rate: The task's declared ``rate_limit`` -- typically a rate
                string such as ``"10/s"``, ``"100/m"`` or ``"1000/h"`` (the
                same value carried by ``Task.rate_limit``).  An unset/zero
                rate **must** be treated by implementations as a no-op
                (always allowed).

        Returns:
            tuple[bool, float]: ``(allowed, retry_after)`` where ``allowed``
            is ``True`` if a token was consumed and the task may run now, and
            ``retry_after`` is the number of seconds the caller should wait
            before re-queueing the task when denied.  ``retry_after`` is
            ``0.0`` whenever ``allowed`` is ``True``.  An unset/zero rate is a
            no-op and **must** return ``(True, 0.0)`` without consulting the
            backend.

        Raises:
            RateLimiterUnavailable: Concrete subclasses **must** raise this
                (rather than return a decision) when their shared backend is
                unavailable, to instruct the caller to fall back to the
                existing per-process rate limiting for this task.  An
                enforced grant or denial is returned as a value; only an
                unavailable backend raises.  See :class:`RateLimiterUnavailable`
                for why fallback must be distinguishable from a real grant.
            NotImplementedError: Always, in the abstract base class.  Concrete
                subclasses override this method with a real implementation.
        """
        raise NotImplementedError
