from unittest import TestCase

from mock import Mock, call

from publisher.job import PublisherJob
from publisher.job import Task

SPLIT_MESSAGES = [
    ['message0', 'message1'],
    ['message2', 'message3'],
    ['message4']
]

TOPIC = 'topic'

CONCURRENCY_LEVEL = 99999


class TestPublisherJob(TestCase):
    def setUp(self):
        self.messages = Mock()
        self.messages.__div__ = Mock(return_value=SPLIT_MESSAGES)
        self.pool = Mock()
        self.kafka_producer = Mock()
        self.kafka_producer.__enter__ = Mock()
        self.kafka_producer.__exit__ = Mock()
        self.job = PublisherJob(self.pool, self.messages, TOPIC, self.kafka_producer, CONCURRENCY_LEVEL)

    def test_should_divide_messages_by_concurrency_level(self):
        self.job.run()
        self.messages.__div__.assert_called_once_with(CONCURRENCY_LEVEL)

    def test_should_submit_tasks_with_split_messages_await_and_close_kafka_publisher(self):
        self.job.run()
        self.pool.assert_has_calls(self.expected_submit_and_await_calls())
        self.kafka_producer.__exit__.assert_called_once()

    def expected_submit_and_await_calls(self):
        calls = self.submit_calls()
        calls.append(call.await())
        return calls

    def submit_calls(self):
        def to_submit_call(messages_split):
            task = Task(messages_split, TOPIC, self.kafka_producer)
            return call.submit(task)

        return map(to_submit_call, SPLIT_MESSAGES)


TASK_MESSAGES = ['message0', 'message1', 'message2']


class TestTask(TestCase):

    def setUp(self):
        self.kafka_producer = Mock()
        self.task = Task(TASK_MESSAGES, TOPIC, self.kafka_producer)

    def test_should_send_messages_to_kafka(self):
        self.task()
        self.kafka_producer.assert_has_calls(expected_send_calls())


def expected_send_calls():
    def to_kafka_send_call(message):
        return call.send(TOPIC, value=message)

    return map(to_kafka_send_call, TASK_MESSAGES)
