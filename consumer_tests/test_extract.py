from unittest import TestCase

from mock import Mock, call

from consumer import KAFKA_SOURCE_FORMAT, KAFKA_BOOTSTRAP_SERVERS_OPTION, KAFKA_TOPIC_OPTION
from consumer.extract import ExtractKafkaMessageValue

LOADED = 'loaded'
BOOTSTRAP_SERVERS_ARGUMENT = 'bootstrap servers argument'
TOPIC_ARGUMENT = 'topic'


class TestExtractKafka(TestCase):
    def setUp(self):
        self.reader = Mock()
        self.reader.option = Mock(return_value=self.reader)
        self.reader.format = Mock(return_value=self.reader)
        self.reader.load = Mock(return_value=LOADED)
        self.arguments = Mock()
        self.arguments.kafka_bootstrap_servers = Mock(return_value=BOOTSTRAP_SERVERS_ARGUMENT)
        self.arguments.kafka_topic = Mock(return_value=TOPIC_ARGUMENT)
        self.extract_kafka = ExtractKafkaMessageValue(self.reader, self.arguments)

    def test_should_return_loaded_value(self):
        result = self.extract_kafka()
        self.assertEqual(LOADED, result)

    def test_should_set_kafka_format(self):
        self.extract_kafka()
        self.reader.format.assert_called_once_with(KAFKA_SOURCE_FORMAT)

    def test_should_set_kafka_BOOTSTRAP_SERVERS_and_TOPIC_options_from_arguments(self):
        self.extract_kafka()
        self.reader.assert_has_calls(expected_option_calls())


def expected_option_calls():
    return [
        call.option(KAFKA_BOOTSTRAP_SERVERS_OPTION, BOOTSTRAP_SERVERS_ARGUMENT),
        call.option(KAFKA_TOPIC_OPTION, TOPIC_ARGUMENT)
    ]
