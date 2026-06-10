"""Standalone Redis-container fixtures for the worker unit tests.

These fixtures spin up a *real* Redis instance in a container (no mocks) so the
global rate-limiter tests can exercise the atomic, server-side Lua token bucket
against a live server.  They intentionally mirror ``t/smoke/conftest.py`` but
omit the docker network / redis.conf volume / custom command, because the unit
test process reaches Redis directly on the host-published port.
"""
import pytest

try:
    import redis
except ImportError:  # pragma: no cover - redis is optional at import time
    redis = None

try:
    from pytest_celery import REDIS_CONTAINER_TIMEOUT, REDIS_ENV, REDIS_IMAGE, REDIS_PORTS, RedisContainer
    from pytest_docker_tools import container, fetch
    _PYTEST_CELERY_AVAILABLE = True
except ImportError:  # pragma: no cover - container infra only present in CI
    _PYTEST_CELERY_AVAILABLE = False


if _PYTEST_CELERY_AVAILABLE:
    redis_image = fetch(repository=REDIS_IMAGE)
    redis_test_container = container(
        image='{redis_image.id}',
        ports=REDIS_PORTS,
        environment=REDIS_ENV,
        wrapper_class=RedisContainer,
        timeout=REDIS_CONTAINER_TIMEOUT,
    )

    @pytest.fixture
    def redis_url(redis_test_container):
        # ``.port`` is the host-published port (reachable from this process);
        # ``.hostname`` would only be valid inside the docker network.
        return f'redis://localhost:{redis_test_container.port}/0'

    @pytest.fixture
    def redis_client(redis_url):
        if redis is None:
            pytest.skip('redis-py is not installed')
        client = redis.from_url(redis_url)
        try:
            yield client
        finally:
            client.close()
