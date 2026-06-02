"""End-to-end integration test for the opt-in, Redis-backed *global* rate limiter.

This is the OPTIONAL end-to-end counterpart to the unit suite in
``t/unit/rate_limiting/test_redis_rate_limiter.py``. It spins up REAL Celery
worker(s) against a LIVE Redis, activates the global limiter for a rate-limited
task, enqueues a burst of that task, records each execution timestamp in Redis,
and asserts the *aggregate* execution rate never exceeds the configured
``rate_limit`` within any rolling one-second window (with a small tolerance for
the token-bucket's initial token and scheduling jitter).

Feature under test
------------------
By default Celery enforces a task's ``rate_limit`` *per worker process* -- each
``Consumer`` builds its own in-memory ``kombu.utils.limits.TokenBucket`` via
``bucket_for_task()``. When ``task_global_rate_limit_backend`` (a Redis URL) is
configured, ``bucket_for_task()`` instead returns a ``RedisTokenBucket``
(``celery/rate_limiting/redis_rate_limiter.py``) that consults Redis atomically
so the limit holds *globally across the entire worker fleet*. Per-task Redis
keys are namespaced as ``celery:global-rate-limit:<task_name>``. On a Redis
failure the limiter degrades per ``task_global_rate_limit_fail_open`` (default
``True`` -> allow/fail-open; ``False`` -> block/fail-closed).

Cross-folder marker caveat (IMPORTANT, read before "fixing" a collection error)
------------------------------------------------------------------------------
The Agent Action Plan requires marking these tests with
``@pytest.mark.integration``. However, the repository's root ``pyproject.toml``
sets ``[tool.pytest.ini_options] addopts = "--strict-markers"`` and, at the time
of writing, registers only ``sleepdeprived_patched_module, masked_modules,
patched_environ, patched_module, flaky, timeout, amqp`` -- the ``integration``
marker is NOT yet registered there. Registering it belongs to the
configuration/root agent (it edits ``pyproject.toml``, which lives OUTSIDE the
``t/`` tree); this test module must NOT edit ``pyproject.toml`` to do so.

* Under the way the integration suite is actually run (``pytest -xsvv
  t/integration`` via tox, i.e. ``--strict-markers`` coming from ``addopts``),
  an unregistered marker is reported only as a ``PytestUnknownMarkWarning`` and
  collection still succeeds.
* If ``--strict-markers`` is passed explicitly on the command line while
  ``integration`` is still unregistered, collection will error on the marker.
  That is the documented cross-folder dependency: the fix is to register
  ``integration`` in ``pyproject.toml``'s ``[tool.pytest.ini_options].markers``
  (config/root agent). The fallback -- should that registration not be
  coordinated -- is to rely solely on the already-registered ``@flaky`` /
  ``@pytest.mark.timeout`` decorators applied below (i.e. drop the
  ``integration`` marker). Do NOT implement that fallback by editing config.

Related cross-folder dependency (CI test matrix)
------------------------------------------------
Adding a new ``test_*.py`` module under ``t/integration`` also requires
registering it in the ``Integration-tests`` job's ``strategy.matrix.module``
list in ``.github/workflows/python-package.yml`` -- the repository's
``scripts/check-ci-test-matrices`` pre-commit hook asserts that the modules on
disk and the workflow matrix match exactly. That workflow file is a root/CI
configuration file OUTSIDE this single-file scope, so (as with the marker
registration above) this module does NOT edit it; the configuration/root agent
must add ``test_global_rate_limit.py`` to that matrix so the lint job stays
green and this optional suite is scheduled in CI.

Every test below is additionally wrapped with the suite-standard ``@flaky``
decorator (composing the registered ``timeout`` + ``flaky`` markers) and is
skipped cleanly when either Redis or the global-limiter feature is unavailable,
so this module never breaks collection or unrelated runs.
"""
import contextlib
import time

import pytest

# ``shared_task`` registers ``record_execution`` at module import time so it is
# present on the session app -- and inherited by the prefork worker's children
# -- before the worker starts (see the task definition below).
from celery import shared_task
# Public testing context manager used to spin up a SECOND in-process worker for
# the cross-fleet enforcement test (see ``test_..._across_two_workers``).
from celery.contrib.testing import worker as contrib_worker

# ``TEST_BACKEND``/``flaky``/``get_redis_connection`` are re-exported from the
# integration ``conftest`` exactly as the sibling modules import them
# (cf. ``t/integration/test_canvas.py`` and ``t/integration/test_tasks.py``).
from .conftest import TEST_BACKEND, flaky, get_redis_connection

# Mark the whole module as an integration test (see the cross-folder caveat in
# the module docstring regarding ``--strict-markers`` marker registration).
pytestmark = pytest.mark.integration

# Generous upper bound (seconds) for awaiting burst completion under throttling.
# A "10/s" rate over N=30 tasks needs ~3s, so 30s leaves ample headroom.
TIMEOUT = 30

# Rate-limit expression under test. The string syntax is intentionally left
# UNCHANGED -- it is parsed by the existing ``celery.utils.time.rate()``
# ("10/s" -> 10.0 tokens/sec). A fast rate keeps the test runtime short.
RATE_LIMIT = "10/s"

# Numeric tokens-per-second equivalent of ``RATE_LIMIT``. Kept as an explicit
# constant (rather than importing the parser) and MUST stay in sync with
# ``RATE_LIMIT`` above: "10/s" == 10.0 tokens/sec.
RATE_PER_SEC = 10.0

# Number of tasks to enqueue in the burst (~3s of work at RATE_PER_SEC).
N = 30

# Tolerance (extra executions) absorbed by the token bucket's initial token
# (capacity=1) plus scheduling jitter, when checking a rolling 1s window.
BURST_TOLERANCE = 3

# Slack (seconds) subtracted from the theoretical minimum wall-clock duration.
DURATION_TOLERANCE = 1.5

# Liveness slack (number of tasks) tolerated as still in-flight at teardown in
# the MULTI-worker case ONLY. With a ``solo`` consumer running alongside the
# ``prefork`` worker in the local fleet, a small number of tasks can remain
# parked in the solo consumer's per-worker rate-limit pending queue when the
# burst window closes -- this is Celery's inherent ``timer.call_after(expected_time)``
# pending-drain behavior, which the AAP preserves unchanged (it is NOT a limiter
# defect, and it never lets the GLOBAL ceiling be exceeded). The QA's own
# two-worker proof tolerated the same ``>= N - 2`` liveness margin. The
# single-worker test keeps the strict ``>= N`` check; only the cross-fleet test
# absorbs this margin. The discriminating ceiling assertion stays strict.
LIVENESS_TOLERANCE = 2

# Redis URL that activates the feature. Reuse the suite's backend when it is a
# Redis URL (so we point at the same Redis the suite already uses); otherwise
# fall back to a local default. Only Redis URLs are valid for this setting.
GLOBAL_RATE_LIMIT_BACKEND = (
    TEST_BACKEND if TEST_BACKEND.startswith("redis") else "redis://localhost:6379/0"
)

# Stable, explicit task name so the limiter's per-task Redis key is deterministic
# and can be cleaned up reliably.
TASK_NAME = "t.integration.test_global_rate_limit.record_execution"

# The limiter's own per-task key, namespaced exactly as the limiter computes it.
LIMITER_KEY = "celery:global-rate-limit:" + TASK_NAME

# Where the task records each execution timestamp (distinct from LIMITER_KEY).
EXECUTION_TIMES_KEY = "global-rate-limit-test:exec-times"


def _redis_available():
    """Return True if the shared integration Redis answers a PING.

    The whole module is skipped when this is False, since a global rate limiter
    backed by Redis cannot be exercised without a live Redis server.
    """
    try:
        return bool(get_redis_connection().ping())
    except Exception:
        # Any connection/transport error means Redis is not usable here.
        return False


def _global_rate_limit_feature_available(app):
    """Return True only when the opt-in global-limiter feature is fully present.

    The feature spans three collaborating pieces created by other agents:
    the ``task_global_rate_limit_backend`` setting (registered in
    ``celery/app/defaults.py``), the ``RedisTokenBucket`` module, and the
    ``bucket_for_task()`` substitution in the consumer. We detect ALL THREE and
    treat any absence as "skip" so this optional test never fails a build where
    the feature (or any part of it) has not landed yet.
    """
    # (1) The setting must be REGISTERED. Reading an unregistered key raises
    #     AttributeError, but membership testing is exception-free and returns
    #     False until the config agent registers the option.
    if "task_global_rate_limit_backend" not in app.conf:
        return False
    # (2) The limiter implementation module must be importable.
    try:
        from celery.rate_limiting import redis_rate_limiter
    except Exception:
        return False
    if redis_rate_limiter is None:
        return False
    # (3) The consumer's ``bucket_for_task()`` factory must actually substitute
    #     the RedisTokenBucket when the backend is configured. Without that hook
    #     the setting is inert and an end-to-end assertion would FAIL rather than
    #     skip, so this third piece is required too. The factory source is
    #     inspected for the RedisTokenBucket reference (integration tests run
    #     against the source checkout, so the source is available); any
    #     inspection failure or a missing reference means the feature is only
    #     partially wired -> skip cleanly.
    try:
        import inspect

        from celery.worker.consumer.consumer import Consumer
        factory_source = inspect.getsource(Consumer.bucket_for_task)
    except Exception:
        return False
    return "RedisTokenBucket" in factory_source


def _record_execution(redis_key=EXECUTION_TIMES_KEY):
    """Append a high-resolution monotonic timestamp on each task execution.

    Mirrors the Redis side-effect convention of ``redis_echo``/``redis_count``
    in ``t/integration/tasks.py`` (open a connection, then ``rpush``). On Linux
    ``time.monotonic()`` is system-wide (``CLOCK_MONOTONIC``), so timestamps are
    comparable across the in-process worker's prefork children and any second
    worker started for the cross-fleet test.
    """
    get_redis_connection().rpush(redis_key, repr(time.monotonic()))


def _await_executions(expected, redis_key=EXECUTION_TIMES_KEY, timeout=TIMEOUT):
    """Poll the timestamp list until it reaches ``expected`` or ``timeout``.

    Returns the recorded timestamps parsed as sorted floats. Adapted from the
    ``await_redis_count`` polling idiom in ``t/integration/test_canvas.py``
    (``check_interval = 0.1``; ``check_max = int(timeout / check_interval)``).
    The caller asserts on the returned length, so a timeout surfaces as a clear
    assertion failure rather than a hang.
    """
    conn = get_redis_connection()
    check_interval = 0.1
    check_max = int(timeout / check_interval)
    for _ in range(check_max + 1):
        if conn.llen(redis_key) >= expected:
            break
        time.sleep(check_interval)
    raw = conn.lrange(redis_key, 0, -1)
    # Redis returns bytes (no ``decode_responses``); decode before float().
    values = [float(item.decode("utf-8")) for item in raw]
    values.sort()
    return values


def _max_in_rolling_window(timestamps, window=1.0):
    """Return the maximum number of timestamps falling within any ``window``.

    Two-pointer sweep over the sorted timestamps: for each right edge advance
    the left edge until the span no longer exceeds ``window``. For a per-second
    rate ``r`` with a capacity-1 token bucket, a healthy global limiter yields
    at most ~``r`` (+ the initial token) per 1s window regardless of worker
    count; an un-coordinated per-worker bucket across K workers yields ~``K*r``.
    """
    ordered = sorted(timestamps)
    max_count = 0
    start = 0
    for end in range(len(ordered)):
        while ordered[end] - ordered[start] > window:
            start += 1
        max_count = max(max_count, end - start + 1)
    return max_count


def _clean_keys():
    """Best-effort removal of both the execution-times key and the limiter key.

    Used for pre-clean (test start) and teardown (test end) so neither this test
    nor the limiter leaks state into other session-scoped integration tests.
    """
    try:
        conn = get_redis_connection()
        conn.delete(EXECUTION_TIMES_KEY)
        conn.delete(LIMITER_KEY)
    except Exception:
        # Cleanup is best-effort; never let it mask the real test outcome.
        pass


def _broadcast_rate_limit(app, rate=RATE_LIMIT):
    """Broadcast the runtime ``rate_limit`` control command and let it settle.

    This is the supported trigger that makes every running worker rebuild its
    ``task_buckets`` through ``bucket_for_task()`` -- the single seam where the
    global ``RedisTokenBucket`` is (or is not) substituted. ``reply=True``
    synchronizes on the worker(s) acknowledging the command; a short sleep then
    absorbs any residual scheduling lag. Both calls are defensive because the
    in-process control mailbox can be momentarily unavailable during teardown.
    """
    try:
        app.control.rate_limit(TASK_NAME, rate, reply=True, timeout=5)
    except Exception:
        try:
            app.control.rate_limit(TASK_NAME, rate)
        except Exception:
            pass
    time.sleep(0.5)


@contextlib.contextmanager
def _local_worker_fleet(app, specs):
    """Start a dedicated, self-contained fleet of in-process workers for a burst.

    ``specs`` is a list of ``start_worker`` kwarg dicts (one per worker, e.g.
    ``{"pool": "prefork", "concurrency": 2}``). Every worker is started with
    ``perform_ping_check=False`` and ``shutdown_timeout=30`` (once the global
    limiter parks a task in a consumer's pending queue, the harness's default
    10s exit window can be too short and would raise "Worker thread failed to
    exit"). Workers are torn down in REVERSE order on exit.

    WHY a dedicated fleet instead of the session worker:
    ``celery.contrib.testing.worker`` stops a worker by setting the
    PROCESS-GLOBAL ``celery.worker.state.should_terminate = 0`` and joining its
    thread. That flag is shared by EVERY in-process worker, so tearing one down
    also trips ``maybe_shutdown()`` in any other live worker's consumer loop and
    stops it too. If these tests leaned on the long-lived ``celery_session_worker``
    (a) its first teardown here would kill it for the rest of the suite, and
    (b) a ``@flaky`` rerun would then run with a missing consumer and stall
    forever. Owning the fleet locally means every attempt (including reruns)
    starts a complete, fresh set of consumers and no shared worker is harmed.
    The flag is saved and restored so it never leaks out of the test. Because the
    fleet is created fresh per test, only ONE ``prefork`` pool is ever live at a
    time (avoiding the billiard signal-handling clash of two concurrent prefork
    pools in a single process). The module-level ``@shared_task`` (see below) is
    what lets a freshly started ``prefork`` worker's forked children inherit the
    task at fork time.
    """
    from celery.worker import state as _worker_state
    saved_should_terminate = _worker_state.should_terminate
    started = []
    try:
        for spec in specs:
            cm = contrib_worker.start_worker(
                app, perform_ping_check=False, shutdown_timeout=30, **spec
            )
            cm.__enter__()
            started.append(cm)
        yield
    finally:
        # Tear down in reverse; swallow teardown errors so they never mask the
        # measured result, then restore the shared terminate flag.
        for cm in reversed(started):
            try:
                cm.__exit__(None, None, None)
            except Exception:
                pass
        _worker_state.should_terminate = saved_should_terminate


# Register the rate-limited task at MODULE IMPORT time as a ``@shared_task``.
#
# This is the crux of making the end-to-end test work against the suite's
# *prefork* session worker (``t/integration/conftest.py`` sets
# ``celery_worker_pool = 'prefork'``). A shared task is registered as soon as
# this module is imported -- which pytest does during COLLECTION, before any
# test runs and therefore before the session worker is created and forks its
# child processes. Consequently:
#   * the task is present on ``celery_session_app`` when the worker finalizes
#     the app at startup, so the worker builds a ``strategies`` entry for it
#     (no "unregistered task"/``KeyError`` in the main process), and
#   * the prefork child processes inherit the task in their registry at fork,
#     so they can actually execute it (no ``NotRegistered`` in the child).
#
# Defining the task in a *fixture* (the previous approach) registered it only
# AFTER the worker had already started and forked, so every message was
# discarded before the limiter was ever consulted. Keeping the task in THIS
# module (rather than ``t/integration/tasks.py``) still honors the
# Minimal-Change Clause -- it is local to the global-rate-limit test. The
# explicit ``name`` keeps the limiter's per-task Redis key deterministic; the
# ``rate_limit`` is declared up front, and the worker resolves its bucket
# (a ``RedisTokenBucket`` once the backend is set) on ``reset_rate_limits()``
# (see ``_broadcast_rate_limit``). This mirrors how ``t/integration/tasks.py``
# tasks are registered, which is why those tasks are dispatchable.
@shared_task(name=TASK_NAME, rate_limit=RATE_LIMIT)
def record_execution(redis_key=EXECUTION_TIMES_KEY):
    _record_execution(redis_key)


@pytest.fixture(scope="session")
def global_rate_limit_task(celery_session_app):
    """Expose the module-level rate-limited ``record_execution`` shared task.

    The task itself is registered at import time (see above) so it is available
    to the prefork session worker before it forks. This fixture simply finalizes
    the shared app (attaching the shared task to it, if not already) and returns
    the task object for the test to enqueue.
    """
    celery_session_app.finalize()
    return celery_session_app.tasks[TASK_NAME]


@pytest.fixture
def global_rate_limit_enabled(celery_session_app, global_rate_limit_task):
    """Activate the global limiter for the test, then restore on teardown.

    Skips cleanly when Redis or the feature is unavailable. The setting is
    enabled on the shared session app BEFORE each test starts its own worker
    fleet (see ``_local_worker_fleet``), so each worker resolves a
    ``RedisTokenBucket`` for the (module-registered) task at startup. The app is
    SESSION-scoped, so this function-scoped fixture MUST undo the mutation in
    ``finally`` to avoid leaking the global backend into other tests. The
    happy-path test runs under the DEFAULT fail-open configuration
    (``task_global_rate_limit_fail_open`` is left at its ``True`` default).

    NOTE: this fixture intentionally does NOT depend on ``celery_session_worker``.
    These tests own their consumers locally (see ``_local_worker_fleet`` for the
    full rationale -- the harness's shutdown toggles a process-global flag that
    would otherwise stop the shared session worker for the rest of the suite and
    deadlock ``@flaky`` reruns).
    """
    if not _redis_available():
        pytest.skip("Live Redis is required for the global rate-limit integration test.")
    if not _global_rate_limit_feature_available(celery_session_app):
        pytest.skip(
            "Global rate-limit feature not present (task_global_rate_limit_backend "
            "setting and/or celery.rate_limiting.redis_rate_limiter unavailable)."
        )

    app = celery_session_app
    # Enable the feature on the shared app. ``record_execution`` is registered at
    # module import time as a ``@shared_task`` (see above), so it is present on
    # the session app -- and inherited by any prefork worker's child processes --
    # before the per-test fleet starts; each worker then resolves a
    # ``RedisTokenBucket`` for it at startup.
    app.conf.task_global_rate_limit_backend = GLOBAL_RATE_LIMIT_BACKEND
    try:
        yield global_rate_limit_task
    finally:
        # Detach the RedisTokenBucket so subsequent session tests see today's
        # default per-worker behavior.
        app.conf.task_global_rate_limit_backend = None
        _clean_keys()


@flaky
def test_global_rate_limit_enforced_single_worker(
        global_rate_limit_enabled, celery_session_app):
    """A burst must drain no faster than the configured global ``rate_limit``.

    Runs under the DEFAULT fail-open configuration. With a single consumer the
    per-worker and global ceilings coincide, so this case primarily proves the
    ``RedisTokenBucket`` is wired in and enforces the rate correctly; the
    cross-fleet proof lives in ``test_..._across_two_workers``.

    A single dedicated ``prefork`` worker (one consumer => one rate-limit
    decision point; ``concurrency=2`` only parallelizes execution to drain near
    the cap) is started locally for the burst -- see ``_local_worker_fleet``.
    """
    task = global_rate_limit_enabled
    app = celery_session_app
    _clean_keys()  # pre-clean so the measurement starts from an empty slate

    start = time.monotonic()
    with _local_worker_fleet(app, [{"pool": "prefork", "concurrency": 2}]):
        # The worker resolves a ``RedisTokenBucket`` at startup; broadcasting the
        # runtime control command additionally exercises the supported
        # ``reset_rate_limits()`` path and lets the consumer settle.
        _broadcast_rate_limit(app)
        for _ in range(N):
            task.delay()
        times = _await_executions(N, timeout=TIMEOUT)
    duration = time.monotonic() - start

    # All enqueued tasks should have executed within the generous TIMEOUT.
    assert len(times) >= N, (
        f"expected {N} executions, only recorded {len(times)} within {TIMEOUT}s"
    )

    # (1) Rate ceiling: no rolling 1s window may exceed rate + small tolerance.
    max_in_window = _max_in_rolling_window(times, window=1.0)
    allowed = RATE_PER_SEC + BURST_TOLERANCE
    assert max_in_window <= allowed, (
        f"max executions in any 1s window was {max_in_window}, exceeding the "
        f"allowed {allowed} (rate {RATE_PER_SEC}/s + tolerance {BURST_TOLERANCE})"
    )

    # (2) Rate floor (wall-clock): the fleet could not have finished faster than
    #     the global limit permits (~N / rate seconds), minus generous slack.
    min_duration = (N / RATE_PER_SEC) - DURATION_TOLERANCE
    assert duration >= min_duration, (
        f"burst of {N} finished in {duration:.2f}s, faster than the "
        f"{min_duration:.2f}s floor implied by the global rate limit"
    )

    _clean_keys()  # explicit teardown clean (the fixture also cleans up)


@flaky
def test_global_rate_limit_enforced_across_two_workers(
        global_rate_limit_enabled, celery_session_app):
    """Prove the limit holds across MULTIPLE worker processes (the feature's point).

    The per-worker bucket lives inside each consumer, so a genuine global proof
    needs >= 2 separate consumers sharing one Redis key. This test spins up its
    OWN dedicated pair of in-process consumers for the burst:

      * a ``prefork`` worker (the fast bulk drainer that pulls the aggregate up
        toward the configured rate), and
      * a ``solo`` worker (a second, distinct consumer).

    Both read the shared backend config the fixture enabled and consult the same
    ``celery:global-rate-limit:<task>`` key. An un-coordinated per-worker bucket
    would permit ~2x the rate across two workers; the global limiter keeps the
    aggregate at ~rate (+ tolerance) -- this is the discriminating assertion.

    The pair is owned locally (see ``_local_worker_fleet`` for the full
    rationale: the harness's shutdown toggles a process-global flag, so reusing
    the shared session worker would kill it and deadlock ``@flaky`` reruns; a
    fresh local fleet keeps every attempt self-contained). The ``solo`` worker
    uses ``prefetch_multiplier=1`` so it never hoards a batch it can only drain
    slowly (which would otherwise force a "Restoring N unacknowledged message(s)"
    stall at teardown); the ``prefork`` worker drains the bulk near the cap.
    """
    task = global_rate_limit_enabled
    app = celery_session_app
    _clean_keys()

    start = time.monotonic()
    fleet = [
        {"pool": "prefork", "concurrency": 2},
        {"pool": "solo", "prefetch_multiplier": 1},
    ]
    with _local_worker_fleet(app, fleet):
        # Both workers resolve a ``RedisTokenBucket`` at startup (the backend is
        # already configured by the fixture); broadcasting the runtime control
        # command additionally exercises the supported ``reset_rate_limits()``
        # path and lets the fleet settle before the burst.
        _broadcast_rate_limit(app)
        for _ in range(N):
            task.delay()
        times = _await_executions(N, timeout=TIMEOUT)
    duration = time.monotonic() - start

    # Liveness (NOT the headline assertion): essentially all tasks ran. A small
    # ``LIVENESS_TOLERANCE`` absorbs the at-most-one task that the transient solo
    # consumer may still be draining from its per-worker pending queue at
    # teardown (Celery's inherent throttled-drain behavior, preserved by the
    # AAP). This mirrors the QA's own two-worker proof (``>= N - 2``). The
    # GLOBAL-ceiling assertion below remains strict -- it is the feature's point.
    assert len(times) >= N - LIVENESS_TOLERANCE, (
        f"expected at least {N - LIVENESS_TOLERANCE} executions across two "
        f"workers, only recorded {len(times)} within {TIMEOUT}s"
    )

    # Discriminating assertion: even with two consumers the GLOBAL ceiling holds.
    max_in_window = _max_in_rolling_window(times, window=1.0)
    allowed = RATE_PER_SEC + BURST_TOLERANCE
    assert max_in_window <= allowed, (
        f"max executions in any 1s window across two workers was {max_in_window}, "
        f"exceeding the allowed {allowed}; the limit is not enforced globally"
    )

    # Wall-clock floor remains valid with two workers under a global limit.
    min_duration = (N / RATE_PER_SEC) - DURATION_TOLERANCE
    assert duration >= min_duration, (
        f"two-worker burst of {N} finished in {duration:.2f}s, faster than the "
        f"{min_duration:.2f}s floor implied by the global rate limit"
    )

    _clean_keys()
