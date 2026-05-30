"""Unit tests for the consumer's global-rate-limiter delegation hook.

These tests exercise ``Consumer.bucket_for_task`` (the single integration
site for the Redis-backed global rate limiter) and the
``Consumer.reset_rate_limits`` re-invocation loop.  All Redis/broker
interaction is mocked: the rate-limiter factory is patched at the name bound
inside the consumer module, so no real factory, Redis, or broker is touched.
"""
import inspect
import socket
from unittest.mock import MagicMock, Mock, call, patch

import pytest

from celery.contrib.testing.mocks import ContextMock
from celery.worker.consumer.consumer import Consumer

# The consumer imports the factory into its OWN namespace
# (``from celery.utils.rate_limit import get_rate_limiter_for_task``), so the
# bound name must be patched here -- patching celery.utils.rate_limit.* would
# not affect the already-bound reference.
FACTORY = 'celery.worker.consumer.consumer.get_rate_limiter_for_task'


def _amqp_connection():
    connection = ContextMock(name='Connection')
    connection.return_value = ContextMock(name='connection')
    connection.return_value.transport.driver_type = 'amqp'
    return connection


class ConsumerTestCase:
    """Mirror of the construction helper in ``test_consumer.py``."""

    def get_consumer(self, no_hub=False, **kwargs):
        consumer = Consumer(
            on_task_request=Mock(),
            init_callback=Mock(),
            pool=Mock(),
            app=self.app,
            timer=Mock(),
            controller=Mock(),
            hub=None if no_hub else Mock(),
            **kwargs
        )
        consumer.blueprint = Mock(name='blueprint')
        consumer.pool.num_processes = 2
        consumer._restart_state = Mock(name='_restart_state')
        consumer.connection = _amqp_connection()
        consumer.connection_errors = (socket.error, OSError,)
        consumer.conninfo = consumer.connection
        return consumer


class test_bucket_for_task(ConsumerTestCase):
    def test_delegates_to_factory_exactly_once(self):
        with patch(FACTORY) as factory:
            consumer = self.get_consumer()
            # __init__ already invoked reset_rate_limits() -> the factory; reset
            # so we can assert exactly-once for this explicit call.
            factory.reset_mock()
            task = Mock(name='task_type')
            consumer.bucket_for_task(task)
            factory.assert_called_once_with(consumer.app, task)

    @pytest.mark.parametrize('bucket', [None, object(), MagicMock(name='bucket')])
    def test_returns_factory_value_verbatim(self, bucket):
        with patch(FACTORY) as factory:
            consumer = self.get_consumer()
            factory.reset_mock()
            factory.return_value = bucket
            result = consumer.bucket_for_task(Mock(name='task_type'))
            assert result is bucket

    def test_delegates_even_when_rate_limit_is_none(self):
        # The consumer no longer inspects rate_limit; the factory decides None.
        with patch(FACTORY) as factory:
            consumer = self.get_consumer()
            factory.reset_mock()
            factory.return_value = None
            task = Mock(name='task_type')
            task.rate_limit = None
            assert consumer.bucket_for_task(task) is None
            factory.assert_called_once_with(consumer.app, task)

    def test_reset_rate_limits_reinvokes_for_every_task(self):
        @self.app.task(shared=False)
        def task_a():
            return 'a'

        @self.app.task(shared=False)
        def task_b():
            return 'b'

        with patch(FACTORY) as factory:
            consumer = self.get_consumer()
            factory.reset_mock()
            consumer.reset_rate_limits()

            # one delegation per registered task (built-ins included)
            assert factory.call_count == len(consumer.app.tasks)
            # always called with the app as the first positional arg
            assert all(c.args[0] is consumer.app
                       for c in factory.call_args_list)
            # the registered custom tasks flowed through as the 2nd arg
            assert call(consumer.app, task_a) in factory.call_args_list
            assert call(consumer.app, task_b) in factory.call_args_list
            # task_buckets rebuilt, keyed by every registered task name
            assert set(consumer.task_buckets) == set(consumer.app.tasks)

    def test_signature_is_unchanged(self):
        # Public API Freeze: bucket_for_task(self, type) must keep its shape.
        params = list(inspect.signature(Consumer.bucket_for_task).parameters)
        assert params == ['self', 'type']
