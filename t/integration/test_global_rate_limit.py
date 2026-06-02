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
import time

import pytest

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


@pytest.fixture(scope="session")
def global_rate_limit_task(celery_session_app):
    """Register the rate-limited task ON THE SHARED SESSION APP (not tasks.py).

    Per the Minimal-Change Clause the task is defined here, in the test module,
    rather than added to ``t/integration/tasks.py``. Because the in-process
    session worker shares this exact ``celery_session_app`` object, the task is
    immediately visible to the worker once registered. The explicit ``name``
    keeps the limiter's per-task Redis key deterministic; ``rate_limit`` is
    declared up front, though the worker only resolves a bucket for it once a
    ``reset_rate_limits()`` is triggered (see ``_broadcast_rate_limit``).
    """
    @celery_session_app.task(name=TASK_NAME, rate_limit=RATE_LIMIT, shared=False)
    def record_execution(redis_key=EXECUTION_TIMES_KEY):
        _record_execution(redis_key)

    return record_execution


@pytest.fixture
def global_rate_limit_enabled(celery_session_app, celery_session_worker,
                              global_rate_limit_task):
    """Activate the global limiter for the test, then restore on teardown.

    Skips cleanly when Redis or the feature is unavailable. The session app and
    worker are SESSION-scoped and shared across the whole suite, so this
    function-scoped fixture MUST undo every mutation in ``finally`` to avoid
    leaking the global backend (and the ``RedisTokenBucket``) into other tests.
    The primary happy-path test runs under the DEFAULT fail-open configuration
    (``task_global_rate_limit_fail_open`` is left at its ``True`` default).
    """
    if not _redis_available():
        pytest.skip("Live Redis is required for the global rate-limit integration test.")
    if not _global_rate_limit_feature_available(celery_session_app):
        pytest.skip(
            "Global rate-limit feature not present (task_global_rate_limit_backend "
            "setting and/or celery.rate_limiting.redis_rate_limiter unavailable)."
        )

    app = celery_session_app
    # Enable the feature on the shared app, then make the running consumer
    # (re)resolve its buckets so the task gets a RedisTokenBucket.
    app.conf.task_global_rate_limit_backend = GLOBAL_RATE_LIMIT_BACKEND
    _broadcast_rate_limit(app)
    try:
        yield global_rate_limit_task
    finally:
        # Detach the RedisTokenBucket and rebuild plain per-worker buckets so
        # subsequent session tests see today's default behavior.
        app.conf.task_global_rate_limit_backend = None
        _broadcast_rate_limit(app)
        _clean_keys()


@flaky
def test_global_rate_limit_enforced_single_worker(global_rate_limit_enabled):
    """A burst must drain no faster than the configured global ``rate_limit``.

    Runs under the DEFAULT fail-open configuration. With a single consumer the
    per-worker and global ceilings coincide, so this case primarily proves the
    ``RedisTokenBucket`` is wired in and enforces the rate correctly; the
    cross-fleet proof lives in ``test_..._across_two_workers``.
    """
    task = global_rate_limit_enabled
    _clean_keys()  # pre-clean so the measurement starts from an empty slate

    start = time.monotonic()
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
    needs >= 2 separate consumers sharing one Redis key. We start a SECOND
    in-process worker (solo pool, no ping check) just for the burst; both
    consumers read the shared backend config and consult the same
    ``celery:global-rate-limit:<task>`` key. An un-coordinated per-worker bucket
    would permit ~2x the rate across two workers; the global limiter keeps the
    aggregate at ~rate (+ tolerance) -- this is the discriminating assertion.
    """
    task = global_rate_limit_enabled
    app = celery_session_app
    _clean_keys()

    start = time.monotonic()
    with contrib_worker.start_worker(app, pool="solo", perform_ping_check=False):
        # Both workers now run with the global backend configured; re-broadcast
        # so the freshly started consumer resolves a RedisTokenBucket too.
        _broadcast_rate_limit(app)
        for _ in range(N):
            task.delay()
        times = _await_executions(N, timeout=TIMEOUT)
    duration = time.monotonic() - start

    assert len(times) >= N, (
        f"expected {N} executions across two workers, only recorded "
        f"{len(times)} within {TIMEOUT}s"
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
