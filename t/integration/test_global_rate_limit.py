"""Integration tests for the Redis-backed global (cluster-wide) rate limiter.

These tests prove that the opt-in :setting:`worker_rate_limits_global` setting
turns a task's :attr:`~celery.app.task.Task.rate_limit` (for example ``"10/s"``)
into the **aggregate** ceiling across **all** workers rather than a per-worker
limit.  Concretely, scaling from one worker to two must keep the observed
throughput at approximately the configured ``rate_limit`` instead of
``rate_limit * worker_count``.

The feature under test lives in sibling modules that this test only *exercises*
through the real worker pipeline (it never imports the limiter internals
directly):

* ``celery/worker/rate_limits.py`` -- the ``RedisTokenBucket`` subclass and the
  ``get_limiter_client(app)`` factory; the per-task Redis hash key follows the
  scheme ``celery:rate_limit:{app.main or 'celery'}:{task_name}``.
* ``celery/worker/consumer/consumer.py`` -- ``bucket_for_task()`` returns a
  ``RedisTokenBucket`` when ``app.conf.worker_rate_limits_global`` is truthy and
  the task declares a ``rate_limit``.
* ``celery/app/defaults.py`` -- the ``worker_rate_limits_global`` (bool) and
  ``worker_rate_limit_url`` (string) settings.

The rate limit is applied at **runtime** through the public broadcast control
command :meth:`app.control.rate_limit`, which exercises requirement R6
(cluster-wide runtime control): the broadcast rebuilds each worker's bucket via
the unchanged ``reset_rate_limits()`` path and, because the bucket state lives
in Redis keyed by task name, the new rate takes effect across the whole cluster.

This is the only F-010 test that runs against a **real** Redis (the unit tests
in ``t/unit/worker/test_rate_limits.py`` mock it).  When Redis is not reachable
the whole module is skipped cleanly rather than erroring.
"""
import os
from contextlib import ExitStack
from time import monotonic, sleep

import pytest

from celery.contrib.testing.worker import start_worker

from .conftest import flaky, get_redis_connection
from .tasks import redis_count

#: Registered name of the shared counter task reused from ``t.integration.tasks``.
TASK_NAME = 't.integration.tasks.redis_count'

#: Default Redis key incremented by :func:`redis_count` (its ``redis_key`` arg).
COUNTER_KEY = 'redis-count'

#: The rate limit under test, expressed in Celery's ``"<n>/<unit>"`` format and
#: as a float for throughput math.  Chosen large enough to be measurable within
#: a few seconds yet clearly below ``RATE_PER_SECOND * 2`` so the cluster-wide
#: and per-worker outcomes are easy to distinguish.
RATE = '10/s'
RATE_PER_SECOND = 10.0

#: Seconds to observe throughput.  Kept >= ~3s so the single initial token
#: (the bucket is built with ``capacity=1``) is diluted and the measured rate
#: settles towards the steady-state fill rate.
MEASURE_WINDOW = 4.0

#: Number of tasks enqueued per measurement.  Must clearly exceed
#: ``RATE_PER_SECOND * MEASURE_WINDOW`` (~40) so the limiter actually throttles,
#: and also exceed the per-worker failure-mode count (~80) so a non-global
#: limiter would be unmistakable.
BURST = 150

#: Seconds to let an ``app.control.rate_limit`` broadcast (and the resulting
#: per-worker bucket rebuild) settle on every worker before measuring.
PROPAGATION_SLEEP = 1.5


def _redis_url():
    """Build the Redis URL pointing at the SAME instance as ``redis_count``.

    The limiter, the counter task and the skip-gate all resolve Redis from the
    ``REDIS_HOST``/``REDIS_PORT`` environment variables, so they coordinate
    through one Redis server and share the per-task bucket state.
    """
    host = os.environ.get('REDIS_HOST', 'localhost')
    port = os.environ.get('REDIS_PORT', 6379)
    return f'redis://{host}:{port}'


# Detect Redis once, at import time, so the whole module skips cleanly (never
# errors) when no Redis is reachable.  ``get_redis_connection().ping()`` opens a
# connection lazily; any failure (connection refused, timeout, ...) is treated
# as "Redis unavailable" for this run.
try:
    get_redis_connection().ping()
    _REDIS_AVAILABLE = True
except Exception:
    _REDIS_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not _REDIS_AVAILABLE,
    reason='Redis is required for the global rate-limit integration test',
)


def _limiter_key(app):
    """Return the per-task limiter hash key, matching ``bucket_for_task``.

    Must equal ``celery:rate_limit:{app.main or 'celery'}:{task_name}`` exactly
    -- the key scheme built by ``celery/worker/rate_limits.py``.  ``app.main`` is
    read dynamically (never hard-coded) so the test stays correct regardless of
    the test app's configured name.
    """
    return f"celery:rate_limit:{app.main or 'celery'}:{TASK_NAME}"


def _reset_state(redis_connection, app):
    """Drop the counter key and the per-task limiter hash.

    Each measurement then starts from a fresh, capacity-full bucket and a zeroed
    counter, keeping the test independent and safely re-runnable.
    """
    redis_connection.delete(COUNTER_KEY)
    redis_connection.delete(_limiter_key(app))


def _measure_aggregate(app, pool, num_workers, redis_connection):
    """Boot ``num_workers`` workers, apply ``RATE``, run a burst, and measure.

    Returns a ``(count, elapsed_seconds)`` tuple where ``count`` is the number
    of ``redis_count`` executions observed within the measurement window.

    The rate limit is applied at runtime through ``app.control.rate_limit`` --
    the primary mechanism exercised here (requirement R6).  Because every worker
    shares the same ``app`` (and therefore the same ``worker_rate_limits_global``
    flag and limiter URL), the rebuilt ``RedisTokenBucket`` instances all point
    at the one Redis hash, which is what makes the limit global.
    """
    with ExitStack() as stack:
        for i in range(num_workers):
            stack.enter_context(
                start_worker(
                    app,
                    pool=pool,
                    concurrency=1,
                    # Distinct nodenames so the workers can coexist.
                    hostname=f'glob_rl_{i}@%h',
                    perform_ping_check=True,
                    loglevel='info',
                    shutdown_timeout=30.0,
                )
            )
        # All workers are up and share the same ``app``.  Apply the rate limit
        # at runtime: the broadcast rebuilds each worker's bucket as a
        # ``RedisTokenBucket`` (the flag is already on), keyed in Redis by task
        # name, yielding a single global bucket for the whole cluster.
        app.control.rate_limit(TASK_NAME, RATE)
        sleep(PROPAGATION_SLEEP)  # let the broadcast + rebuild settle everywhere

        # Fresh bucket + counter for this measurement window.
        _reset_state(redis_connection, app)

        # Fire a burst that clearly exceeds ``RATE * window`` so the limiter
        # must throttle rather than drain the whole burst inside the window.
        start = monotonic()
        for _ in range(BURST):
            redis_count.delay()

        sleep(MEASURE_WINDOW)
        elapsed = monotonic() - start
        count = int(redis_connection.get(COUNTER_KEY) or 0)

    # Workers are stopped here as the ExitStack unwinds.  Drop the queued
    # backlog so the un-processed remainder cannot contaminate the next case.
    try:
        app.control.purge()
    except Exception:
        pass
    return count, elapsed


@flaky
def test_redis_token_bucket_is_cluster_wide(app, celery_worker_pool):
    """Prove the global limiter caps *aggregate* throughput across workers.

    Measures a single-worker baseline and a two-worker aggregate in the same
    environment, then asserts the two-worker throughput stays approximately the
    configured limit (NOT approximately ``limit * 2``).  The discriminating
    check is the ratio of the two-worker count to the single-worker count: ~1
    when the limit is global, ~2 when it is (incorrectly) per-worker.

    Only the ``app`` and ``celery_worker_pool`` fixtures are requested -- never
    ``manager``/``celery_session_worker``, whose session-scoped worker would
    have booted with the feature OFF and would also consume ``redis_count``
    tasks, corrupting the measurement.
    """
    redis_connection = get_redis_connection()

    # Enable the feature on the shared ``app`` BEFORE any worker boots, and
    # remember the originals so the session-scoped fixtures and other tests are
    # left untouched.  ``worker_rate_limit_url`` is set explicitly (rather than
    # left to the result_backend/broker_url fallback) so the test is robust even
    # under toxenvs where neither of those is a Redis URL.
    original_flag = app.conf.worker_rate_limits_global
    original_url = app.conf.worker_rate_limit_url
    original_rate = redis_count.rate_limit
    app.conf.worker_rate_limits_global = True
    app.conf.worker_rate_limit_url = _redis_url()
    try:
        # Single-worker baseline: throughput should be ~= the configured limit.
        single_count, single_elapsed = _measure_aggregate(
            app, celery_worker_pool, 1, redis_connection)

        # Two workers against the SAME Redis: the aggregate must stay ~= the
        # limit, i.e. clearly NOT ~= limit * 2 (the per-worker failure mode).
        two_count, two_elapsed = _measure_aggregate(
            app, celery_worker_pool, 2, redis_connection)

        single_rate = single_count / single_elapsed
        two_rate = two_count / two_elapsed

        # 1) Tasks actually flowed (the limiter neither blocked everything nor
        #    crashed the pipeline).
        assert single_count > 0
        assert two_count > 0

        # 2) Single-worker baseline ~= limit (generous band: the capacity=1
        #    bucket admits a small initial burst before settling to fill rate).
        assert single_rate <= RATE_PER_SECOND * 1.6, (
            f'single-worker rate {single_rate:.2f}/s exceeded ~limit '
            f'(count={single_count}, elapsed={single_elapsed:.2f}s)')

        # 3) Two-worker aggregate ~= limit (NOT ~limit*2): the core
        #    cluster-wide proof.
        assert two_rate <= RATE_PER_SECOND * 1.5, (
            f'two-worker aggregate rate {two_rate:.2f}/s looks per-worker '
            f'(count={two_count}, elapsed={two_elapsed:.2f}s)')

        # 4) Two-worker count is close to the single-worker count, not double
        #    it -- the headline assertion that discriminates global from
        #    per-worker enforcement and is robust to absolute-throughput noise.
        assert two_count < single_count * 1.6, (
            f'two-worker count {two_count} ~= 2x single-worker {single_count}: '
            f'rate limit is NOT cluster-wide')

        # 5) Throughput is actually CLOSE TO the limit, not near zero.  The
        #    upper bounds above are satisfied even by a broken limiter that
        #    admits only a handful of tasks across the window, so a generous
        #    lower bound (>= half the configured rate) on BOTH measurements is
        #    what proves throughput ~= limit rather than merely "not above it".
        assert single_rate >= RATE_PER_SECOND * 0.5, (
            f'single-worker rate {single_rate:.2f}/s is far below ~limit '
            f'(count={single_count}, elapsed={single_elapsed:.2f}s): the '
            f'limiter is under-admitting, not enforcing ~{RATE_PER_SECOND}/s')
        assert two_rate >= RATE_PER_SECOND * 0.5, (
            f'two-worker aggregate rate {two_rate:.2f}/s is far below ~limit '
            f'(count={two_count}, elapsed={two_elapsed:.2f}s): the limiter is '
            f'under-admitting, not enforcing ~{RATE_PER_SECOND}/s')

        # 6) Two-sided ratio band: the two-worker count must be neither ~2x the
        #    single-worker baseline (per-worker failure mode, bounded above by
        #    check 4) nor drastically lower (an over-throttling/broken limiter).
        #    Combined with check 4 this pins two_count / single_count into
        #    [0.6, 1.6) -- i.e. ~1, the signature of one shared global bucket.
        assert two_count >= single_count * 0.6, (
            f'two-worker count {two_count} is far below single-worker '
            f'{single_count} (ratio {two_count / single_count:.2f} < 0.6): the '
            f'global bucket appears to be over-throttling across two workers')
    finally:
        # Restore global state so the session-scoped fixtures and other tests
        # are unaffected, and leave Redis clean for back-to-back re-runs.
        app.conf.worker_rate_limits_global = original_flag
        app.conf.worker_rate_limit_url = original_url
        redis_count.rate_limit = original_rate
        _reset_state(redis_connection, app)
        try:
            app.control.purge()
        except Exception:
            pass
