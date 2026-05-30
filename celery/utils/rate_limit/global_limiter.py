"""Redis-backed global (cluster-wide) token-bucket rate limiter."""
from kombu.utils.limits import TokenBucket

from celery.utils.log import get_logger

logger = get_logger(__name__)

# Atomic token-bucket Lua script.
#
# The whole read -> refill -> consume -> write cycle runs inside a SINGLE
# ``EVAL`` invocation, which Redis executes as one indivisible command.  That
# atomicity is exactly what elevates this limiter from a per-worker-process cap
# to a cluster-wide cap: no two workers can ever observe (and therefore
# double-spend) the same intermediate bucket state.
#
# Contract:
#   KEYS = [bucket_key]                       -- "celery:rate:<task_name>"
#   ARGV = [rate, capacity, tokens, consume]  -- rate=tokens/sec (float),
#                                                capacity=bucket size,
#                                                tokens=request size (usually 1),
#                                                consume=1 to spend tokens on a
#                                                successful check (can_consume),
#                                                or 0 for a non-consuming peek
#                                                that only refills timing state
#                                                (expected_time).
#   returns {allowed, wait_seconds}           -- allowed is 1 or 0;
#                                                wait_seconds is a STRING.
#
# Notable properties (see AAP 0.5.4 / 0.7.6):
#   * Single-clock arithmetic -- every ``now`` comes from ``redis.call('TIME')``
#     so workers with skewed system clocks all agree on the refill schedule.
#   * Idempotent first call -- a missing hash seeds a *full* bucket, matching the
#     in-memory ``TokenBucket`` whose initial ``_tokens == capacity``.
#   * TTL self-maintenance -- every write refreshes ``EXPIRE`` so idle tasks free
#     their key, while continuously active tasks keep it alive.
#   * Bounded key namespace -- the script touches ONLY ``KEYS[1]``.
#   * Float precision -- token counters and the returned wait are stringified so
#     Redis does not truncate the floating-point values in transit.
_LUA_TOKEN_BUCKET = """
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local request = tonumber(ARGV[3])
-- consume=1 -> spend a token on success (can_consume); consume=0 -> non-consuming
-- peek (expected_time): refill/write timing state but never decrement.
local consume = tonumber(ARGV[4])

-- Server-side clock ONLY: eliminates worker clock skew (every `now` comes from here).
local t = redis.call('TIME')
local now = tonumber(t[1]) + (tonumber(t[2]) / 1000000)

-- Read current bucket state; seed a full bucket on first use (idempotent first call).
local data = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens_stored = tonumber(data[1])
local last_refill = tonumber(data[2])
if tokens_stored == nil or last_refill == nil then
    tokens_stored = capacity
    last_refill = now
end

-- Refill against elapsed server time, capped at capacity.
local elapsed = now - last_refill
local new_tokens = math.min(capacity, tokens_stored + (elapsed * rate))

-- TTL so idle tasks free their key; refreshed on every write.
local ttl = math.ceil((2 * capacity) / rate)
if ttl < 1 then
    ttl = 1
end

local allowed = 0
local wait = 0
if new_tokens >= request then
    allowed = 1
    -- Only spend a token when the caller intends to CONSUME (can_consume).
    -- expected_time passes consume=0 so it acts as a non-consuming peek: the
    -- consumer's scheduling loop has already requeued the request before
    -- calling expected_time, so decrementing here would lose capacity and
    -- under-dispatch. Mirrors kombu TokenBucket.expected_time, which refills
    -- via _get_tokens but never decrements.
    if consume == 1 then
        new_tokens = new_tokens - request
    end
else
    wait = (request - new_tokens) / rate
end

-- Persist as strings to preserve float precision (Lua number args can truncate).
redis.call('HSET', key, 'tokens', tostring(new_tokens), 'last_refill', tostring(now))
redis.call('EXPIRE', key, ttl)

-- Return wait as a STRING: Redis converts Lua numbers in replies to integers
-- (floats would be truncated), so a float wait must be stringified here and
-- parsed back with float() on the Python side.
return {allowed, tostring(wait)}
"""


class GlobalRateLimiter(TokenBucket):
    """TokenBucket whose token state is shared cluster-wide via Redis.

    Overrides only ``can_consume`` and ``expected_time`` to consult an atomic
    Lua script keyed on ``celery:rate:<task_name>``.  All other TokenBucket
    behaviour (``add``/``pop``/``clear_pending``/``contents``) is inherited so
    the consumer's scheduling loop works unchanged.  Any Redis failure degrades
    gracefully to the in-memory parent implementation for that single call.
    """

    def __init__(self, fill_rate, redis_client, task_name, capacity=None):
        # GOTCHA: TokenBucket.__init__ does float(capacity); float(None) raises
        # TypeError, so compute a sane default BEFORE calling super().
        if capacity is None:
            capacity = max(1, int(fill_rate))
        # Call super FIRST so all parent state (.contents/.fill_rate/.capacity/
        # ._tokens/.timestamp) exists for the in-memory fallback paths.
        super().__init__(fill_rate, capacity)
        self._redis = redis_client
        self._key = f"celery:rate:{task_name}"
        self._script = redis_client.register_script(_LUA_TOKEN_BUCKET)

    def can_consume(self, tokens=1):
        try:
            # consume=1: spend the requested tokens on success (consuming check),
            # matching kombu TokenBucket.can_consume which decrements on success.
            result = self._script(
                keys=[self._key],
                args=[self.fill_rate, self.capacity, tokens, 1],
            )
            return bool(result[0])
        except Exception as exc:
            # R5 graceful fallback: log (NO URL -- 0.7.6) and use the in-memory
            # parent bucket for this single call so dispatch never raises.
            logger.warning(
                'Global rate limiter %r can_consume failed (%s: %s); '
                'falling back to in-memory bucket.',
                self._key, exc.__class__.__name__, exc)
            return super().can_consume(tokens)

    def expected_time(self, tokens=1):
        # NON-consuming peek (consume=0): the consumer calls expected_time only
        # after can_consume returned False and the request was requeued, so this
        # must NOT spend a token (doing so loses capacity / under-dispatches).
        # Mirrors kombu TokenBucket.expected_time, which refills but never
        # decrements; it still refreshes the refill timestamp/TTL via the script.
        try:
            result = self._script(
                keys=[self._key],
                args=[self.fill_rate, self.capacity, tokens, 0],
            )
            return float(result[1])
        except Exception as exc:
            # R5 graceful fallback: log (NO URL -- 0.7.6) and use the in-memory
            # parent bucket for this single call so dispatch never raises.
            logger.warning(
                'Global rate limiter %r expected_time failed (%s: %s); '
                'falling back to in-memory bucket.',
                self._key, exc.__class__.__name__, exc)
            return super().expected_time(tokens)

    def __repr__(self):
        return f'<GlobalRateLimiter: {self._key} {self.fill_rate}/s>'
