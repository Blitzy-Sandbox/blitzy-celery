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

__all__ = ('BaseRateLimiter',)


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
            ``0.0`` whenever ``allowed`` is ``True``.

        Raises:
            NotImplementedError: Always, in the abstract base class.  Concrete
                subclasses override this method with a real implementation.
        """
        raise NotImplementedError
