"""Redis-backed global (cluster-wide) rate limiting."""
from .factory import get_rate_limiter_for_task
from .global_limiter import GlobalRateLimiter

__all__ = ("get_rate_limiter_for_task", "GlobalRateLimiter")
