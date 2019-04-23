import json
from datetime import datetime
from unittest import TestCase

from pyspark.sql import SparkSession
from pyspark.sql.types import BinaryType, StructType, StringType, Row, StructField, IntegerType

from consumer import extract
from consumer.transform import ParseJsonColumnTransform, AddTimestampColumnDecoratorTransform


class TestSparkIntegration(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName('sparkIntegrationTest') \
            .master('local') \
            .config("spark.sql.shuffle.partitions", 1) \
            .config("spark.ui.enabled", False) \
            .getOrCreate()


_FIRST_RECORD = 0
_FIRST_COLUMN = 0


class TestConvertBytesToString(TestSparkIntegration):

    def test_should_convert_bytes_to_string(self):
        expected_data = 'data'

        def read_raw_bytes():
            data_as_bytes = bytearray(expected_data)
            return self.spark \
                .createDataFrame([data_as_bytes], schema=BinaryType())

        wrapped_read_raw_bytes = extract.bytes_to_string(read_raw_bytes)
        result = wrapped_read_raw_bytes() \
            .collect()[_FIRST_RECORD][_FIRST_COLUMN]
        self.assertEqual(expected_data, result)


STRING_COLUMN = 'first_column'
INTEGER_COLUMN = 'second_column'

EXPECTED_STRING_VALUE = 'expected'
EXPECTED_INTEGER_VALUE = 0

JSON_DATA = {
    STRING_COLUMN: EXPECTED_STRING_VALUE,
    INTEGER_COLUMN: EXPECTED_INTEGER_VALUE
}

JSON_COLUMN_NAME = 'json'


class TestParseJsonColumnTransform(TestSparkIntegration):

    def setUp(self):
        schema = StructType(
            [
                StructField(STRING_COLUMN, StringType()),
                StructField(INTEGER_COLUMN, IntegerType())
            ]
        )
        self.transform = ParseJsonColumnTransform(JSON_COLUMN_NAME, schema)

    def test_should_extract_properties_with_names(self):
        result = self.transform(self.input()) \
            .collect()[_FIRST_RECORD]
        self.assertEqual(JSON_DATA[STRING_COLUMN], result[STRING_COLUMN])
        self.assertEqual(JSON_DATA[INTEGER_COLUMN], result[INTEGER_COLUMN])

    def input(self):
        json_record = json.dumps(JSON_DATA)
        schema = StructType() \
            .add(JSON_COLUMN_NAME, StringType())

        return self.spark.createDataFrame([Row(json_record)], schema)


TIMESTAMP_FIELD_NAME = 'timestamp'
ORIGINAL = 'original'
TRANSFORMED = 'transformed'
EXISTING_VALUE = 'existing'


class Delegatee(object):
    def __init__(self, spark):
        self.spark = spark

    def __call__(self, *args, **kwargs):
        input = args[0]
        return input.withColumnRenamed(ORIGINAL, 'transformed')


class TestAddTimestampColumnDecoratorTransform(TestSparkIntegration):

    def test_should_add_timestamp_to_existing_columns(self):
        decorator = AddTimestampColumnDecoratorTransform(delegatee=Delegatee(self.spark),
                                                         timestamp_field_name=TIMESTAMP_FIELD_NAME)
        result = decorator(self.input()) \
            .collect()[_FIRST_RECORD]
        self.assertEqual(EXISTING_VALUE, result[TRANSFORMED])
        self.assertIsInstance(result[TIMESTAMP_FIELD_NAME], datetime)

    def input(self):
        schema = StructType([StructField(ORIGINAL, StringType())])
        return self.spark.createDataFrame([Row(EXISTING_VALUE)], schema=schema)
