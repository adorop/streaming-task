from functools import wraps

from pyspark.sql.functions import expr

from consumer import KAFKA_SOURCE_FORMAT, KAFKA_BOOTSTRAP_SERVERS_OPTION, KAFKA_TOPIC_OPTION, arguments

KAFKA_MESSAGE_VALUE = 'value'


class ExtractKafkaMessageValue(object):
    def __init__(self, reader, arguments):
        self.reader = reader
        self.arguments = arguments

    def __call__(self, *args, **kwargs):
        return self.reader \
            .format(KAFKA_SOURCE_FORMAT) \
            .option(KAFKA_BOOTSTRAP_SERVERS_OPTION, self.arguments.kafka_bootstrap_servers()) \
            .option(KAFKA_TOPIC_OPTION, self.arguments.kafka_topic()) \
            .load()


def bytes_to_string(extract_kafka_bytes):
    @wraps(extract_kafka_bytes)
    def wrapper(*args, **kwargs):
        message_value_as_string = expr('CAST(value AS STRING)') \
            .alias(KAFKA_MESSAGE_VALUE)
        return extract_kafka_bytes(*args, **kwargs) \
            .select(message_value_as_string)

    return wrapper


class ExtractKafkaMessageValueAsString(ExtractKafkaMessageValue):

    @bytes_to_string
    def __call__(self, *args, **kwargs):
        return super(ExtractKafkaMessageValueAsString, self).__call__(*args, **kwargs)
