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

Why subprocess workers (and not :func:`celery.contrib.testing.worker.start_worker`)
------------------------------------------------------------------------------
The cluster-wide proof requires **two workers running concurrently** against one
Redis.  Booting two embedded workers *in the same process* (two
``threading.Thread`` targets driving ``worker.start``) is not viable here: two
in-process workers sharing one app re-enter the kombu async hub's generator
callbacks and the redis result-backend drainer, which manifests as
``generator already executing`` / ``I/O operation on closed file`` /
``Connection closed by server`` and a shutdown hang -- a *test-harness*
limitation entirely unrelated to the rate limiter's production code.

This test therefore boots each worker as an isolated **subprocess** (a real
``celery ... worker`` process).  Process isolation sidesteps the in-process hub
re-entrancy completely, while still exercising the genuine end-to-end pipeline
(broker -> strategy -> ``bucket_for_task`` -> ``RedisTokenBucket`` -> atomic Lua
in Redis).  The single module-level :data:`worker_app` is used both as the
``-A`` target for the subprocess workers and, in the test process, as the
producer / control client, so the producer and consumers always share one
broker, backend and limiter Redis.  A ``prefork`` pool is used (matching the
``celery_worker_pool`` fixture) so task execution happens in child processes and
never blocks the consumer's rate-limit reschedule timer.

This is the only F-010 test that runs against a **real** Redis (the unit tests
in ``t/unit/worker/test_rate_limits.py`` mock it).  When Redis is not reachable
the whole module is skipped cleanly rather than erroring.
"""
import os
import subprocess
import sys
from signal import SIGKILL
from time import monotonic, sleep

import pytest

from celery import Celery

from .conftest import flaky, get_redis_connection
from .tasks import redis_count

#: Registered name of the shared counter task reused from ``t.integration.tasks``.
TASK_NAME = redis_count.name

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

#: Maximum seconds to wait for the requested number of subprocess workers to
#: come up and answer a control ``ping`` before giving up.
WORKER_READY_TIMEOUT = 40.0

#: Seconds to wait for a subprocess worker to exit on ``terminate`` before the
#: whole process group is force-killed.
WORKER_SHUTDOWN_TIMEOUT = 20.0

#: The default queue the workers consume and the producer sends to.
DEFAULT_QUEUE = 'celery'

#: URL schemes recognised as "this is a Redis URL we can reuse verbatim".
_REDIS_SCHEMES = ('redis://', 'rediss://')


def _redis_url():
    """Build the Redis URL pointing at the SAME instance as ``redis_count``.

    The limiter, the counter task and the skip-gate all resolve Redis from the
    ``REDIS_HOST``/``REDIS_PORT`` environment variables, so they coordinate
    through one Redis server and share the per-task bucket state.
    """
    host = os.environ.get('REDIS_HOST', 'localhost')
    port = os.environ.get('REDIS_PORT', 6379)
    return f'redis://{host}:{port}'


def _broker_url():
    """Resolve the broker URL for :data:`worker_app`.

    Honour ``TEST_BROKER`` when it is a Redis URL (the integration-redis
    toxenv), otherwise fall back to the Redis the skip-gate already requires.
    Because the producer and the consumers both use :data:`worker_app`, this URL
    is shared by construction regardless of the ambient broker configuration.
    """
    url = os.environ.get('TEST_BROKER', '')
    if url.startswith(_REDIS_SCHEMES):
        return url
    return _redis_url() + '/0'


def _backend_url():
    """Resolve the result backend URL for :data:`worker_app` (Redis)."""
    url = os.environ.get('TEST_BACKEND', '')
    if url.startswith(_REDIS_SCHEMES):
        return url
    return _redis_url() + '/0'


# The single coordinating app for this module.  It is created with
# ``set_as_current=False`` so merely importing this module (which the celery
# subprocess does to resolve ``-A ...:worker_app``) never mutates global Celery
# state or the pytest ``celery_app`` fixture.  The feature is enabled here, on
# this app, so every subprocess worker booted from it builds RedisTokenBuckets
# pointing at one shared Redis key.  ``task_ignore_result`` keeps the result
# backend free of throwaway ``redis_count`` results.
worker_app = Celery(
    't.integration.test_global_rate_limit',
    broker=_broker_url(),
    backend=_backend_url(),
    include=['t.integration.tasks'],
    set_as_current=False,
)
worker_app.conf.worker_rate_limits_global = True
worker_app.conf.worker_rate_limit_url = _redis_url()
worker_app.conf.task_ignore_result = True


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


def _limiter_key():
    """Return the per-task limiter hash key, matching ``bucket_for_task``.

    Must equal ``celery:rate_limit:{app.main or 'celery'}:{task_name}`` exactly
    -- the key scheme built by ``celery/worker/rate_limits.py``.  ``app.main`` is
    read dynamically from :data:`worker_app` (the app the subprocess workers
    actually run) so the reset always targets the bucket the workers use.
    """
    return f"celery:rate_limit:{worker_app.main or 'celery'}:{TASK_NAME}"


def _reset_state(redis_connection):
    """Drop the counter key and the per-task limiter hash.

    Each measurement then starts from a fresh, capacity-full bucket and a zeroed
    counter, keeping the test independent and safely re-runnable.
    """
    redis_connection.delete(COUNTER_KEY)
    redis_connection.delete(_limiter_key())


def _purge_queue():
    """Best-effort drain of the default queue so no backlog bleeds across cases.

    A burst of ``BURST`` tasks is never fully drained inside one window; the
    remainder would otherwise be consumed by the next measurement's workers and
    inflate its count.  Both a broadcast ``purge`` (for any live workers) and a
    direct delete of the broker's Redis list are attempted; either failing is
    harmless.
    """
    try:
        worker_app.control.purge()
    except Exception:
        pass
    try:
        with worker_app.connection_for_write() as conn:
            conn.default_channel.client.delete(DEFAULT_QUEUE)
    except Exception:
        pass


def _ping(timeout=1.5):
    """Return the list of control-``ping`` replies, or ``[]`` on any error."""
    try:
        return worker_app.control.ping(timeout=timeout) or []
    except Exception:
        return []


def _shutdown_lingering_workers(timeout=10.0):
    """Shut down any workers still listening on the broker before measuring.

    Guards against orphans left by a prior ``@flaky`` rerun: a leftover worker
    would both answer ``ping`` (masking readiness) and consume ``redis_count``
    tasks (corrupting the count).  Only workers booted from :data:`worker_app`
    listen on this broker, so the broadcast cleanly targets just our workers.
    """
    if not _ping():
        return
    try:
        worker_app.control.shutdown()
    except Exception:
        pass
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if not _ping():
            return
        sleep(0.5)


def _spawn_worker(index, pool):
    """Launch one isolated ``celery worker`` subprocess bound to ``worker_app``.

    Process isolation is what makes running two workers concurrently safe (see
    the module docstring).  ``start_new_session=True`` makes the worker a process
    group leader so :func:`_terminate` can reap the whole tree (prefork children
    included).  ``C_FORCE_ROOT`` is set only when running as root, and the repo
    root is placed on ``PYTHONPATH`` so the subprocess can import this module to
    resolve ``-A t.integration.test_global_rate_limit:worker_app``.
    """
    env = os.environ.copy()
    if getattr(os, 'geteuid', lambda: 1)() == 0:
        env['C_FORCE_ROOT'] = 'true'
    env['PYTHONPATH'] = os.getcwd() + os.pathsep + env.get('PYTHONPATH', '')
    cmd = [
        sys.executable, '-m', 'celery',
        '-A', 't.integration.test_global_rate_limit:worker_app',
        'worker',
        '--pool', pool,
        '--concurrency', '1',
        '-n', f'glob_rl_{index}@%h',
        '-Q', DEFAULT_QUEUE,
        '--without-mingle',
        '--without-gossip',
        '--without-heartbeat',
        '--loglevel', 'warning',
    ]
    return subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def _wait_until_ready(num_workers, timeout=WORKER_READY_TIMEOUT):
    """Poll control ``ping`` until at least ``num_workers`` workers reply.

    Returns the number of workers seen (>= ``num_workers`` on success, ``-1`` on
    timeout).  Using the broker-level ping (rather than waiting on a task result)
    avoids the in-process result-backend drainer entirely.
    """
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        replies = _ping()
        if len(replies) >= num_workers:
            return len(replies)
        sleep(0.5)
    return -1


def _terminate(procs):
    """Stop every spawned worker subprocess, force-killing the group if needed."""
    for proc in procs:
        try:
            proc.terminate()
        except Exception:
            pass
    for proc in procs:
        try:
            proc.wait(timeout=WORKER_SHUTDOWN_TIMEOUT)
        except Exception:
            # Warm shutdown did not complete in time: force-kill the whole
            # process group (start_new_session=True made the worker the group
            # leader, so this also reaps prefork pool children).
            try:
                os.killpg(os.getpgid(proc.pid), SIGKILL)
            except Exception:
                pass
            try:
                proc.wait(timeout=5)
            except Exception:
                pass


def _measure_aggregate(pool, num_workers, redis_connection):
    """Boot ``num_workers`` workers, apply ``RATE``, run a burst, and measure.

    Returns a ``(count, elapsed_seconds)`` tuple where ``count`` is the number
    of ``redis_count`` executions observed within the measurement window.

    The rate limit is applied at runtime through ``app.control.rate_limit`` --
    the primary mechanism exercised here (requirement R6).  Because every worker
    is booted from the same :data:`worker_app` (and therefore shares the
    ``worker_rate_limits_global`` flag and limiter URL), the rebuilt
    ``RedisTokenBucket`` instances all point at the one Redis hash, which is what
    makes the limit global.
    """
    # Start from a clean slate: no leftover workers, no queue backlog, fresh
    # counter and bucket.
    _shutdown_lingering_workers()
    _purge_queue()
    _reset_state(redis_connection)

    procs = [_spawn_worker(i, pool) for i in range(num_workers)]
    try:
        ready = _wait_until_ready(num_workers)
        assert ready >= num_workers, (
            f'expected {num_workers} worker(s) to start and answer ping, '
            f'saw {ready}')

        # Apply the rate limit at runtime: the broadcast rebuilds each worker's
        # bucket as a ``RedisTokenBucket`` (the flag is already on), keyed in
        # Redis by task name, yielding a single global bucket for the cluster.
        worker_app.control.rate_limit(TASK_NAME, RATE)
        sleep(PROPAGATION_SLEEP)  # let the broadcast + rebuild settle everywhere

        # Fresh queue + bucket + counter for this measurement window.
        _purge_queue()
        _reset_state(redis_connection)

        # Fire a burst that clearly exceeds ``RATE * window`` so the limiter
        # must throttle rather than drain the whole burst inside the window.
        start = monotonic()
        for _ in range(BURST):
            worker_app.send_task(TASK_NAME)

        sleep(MEASURE_WINDOW)
        elapsed = monotonic() - start
        count = int(redis_connection.get(COUNTER_KEY) or 0)
    finally:
        # Stop the workers and drop the queued backlog so the un-processed
        # remainder cannot contaminate the next case.
        _terminate(procs)
        _purge_queue()
    return count, elapsed


@flaky
def test_redis_token_bucket_is_cluster_wide(celery_worker_pool):
    """Prove the global limiter caps *aggregate* throughput across workers.

    Measures a single-worker baseline and a two-worker aggregate in the same
    environment, then asserts the two-worker throughput stays approximately the
    configured limit (NOT approximately ``limit * 2``).  The discriminating
    check is the ratio of the two-worker count to the single-worker count: ~1
    when the limit is global, ~2 when it is (incorrectly) per-worker.

    Only the ``celery_worker_pool`` fixture is requested -- never
    ``manager``/``celery_session_worker``, whose session-scoped worker would
    have booted with the feature OFF and would also consume ``redis_count``
    tasks, corrupting the measurement.  The workers under test are isolated
    subprocesses booted from the module-level :data:`worker_app` (which already
    has ``worker_rate_limits_global`` enabled), so no per-test mutation of any
    shared fixture state is required.
    """
    redis_connection = get_redis_connection()
    try:
        # Single-worker baseline: throughput should be ~= the configured limit.
        single_count, single_elapsed = _measure_aggregate(
            celery_worker_pool, 1, redis_connection)

        # Two workers against the SAME Redis: the aggregate must stay ~= the
        # limit, i.e. clearly NOT ~= limit * 2 (the per-worker failure mode).
        two_count, two_elapsed = _measure_aggregate(
            celery_worker_pool, 2, redis_connection)

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
        # Leave Redis clean for back-to-back re-runs and make sure no worker
        # subprocess outlives the test.
        _shutdown_lingering_workers()
        _reset_state(redis_connection)
        _purge_queue()
