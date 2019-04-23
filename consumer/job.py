from commons import domain
from consumer import ELASTICSEARCH_OUTPUT_FORMAT
from consumer.extract import ExtractKafkaMessageValueAsString
from consumer.extract import KAFKA_MESSAGE_VALUE
from consumer.load import StreamLoad, BatchWriteToCsvLoad
from consumer.transform import ParseJsonColumnTransform, AddTimestampColumnDecoratorTransform


class ConsumerJobTemplate(object):

    def __init__(self, extract, transform, load):
        self.extract = extract
        self.transform = transform
        self.load = load

    def run(self):
        extracted = self.extract()
        transformed = self.transform(extracted)
        self.load(transformed)


class ConsumerJob(object):

    def __new__(cls, *args, **kwargs):
        spark = kwargs['spark']
        arguments = args[0]
        reader, load = _get_reader_and_load(spark, arguments)
        return ConsumerJobTemplate(
            ExtractKafkaMessageValueAsString(reader, arguments),
            _get_transform(arguments),
            load
        )


def _get_reader_and_load(spark, arguments):
    if arguments.processing_type() == 'streaming':
        return spark.readStream, StreamLoad(arguments)
    else:
        return spark.read, BatchWriteToCsvLoad(arguments)


def _get_transform(arguments):
    parse_json_transform = ParseJsonColumnTransform(json_column_name=KAFKA_MESSAGE_VALUE, schema=domain.schema())
    if arguments.sink() == ELASTICSEARCH_OUTPUT_FORMAT:
        return AddTimestampColumnDecoratorTransform(delegatee=parse_json_transform, timestamp_field_name='@timestamp')
    else:
        return parse_json_transform
