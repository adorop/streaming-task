import random
from datetime import datetime

from pyspark.sql.types import TimestampType, DateType, BooleanType, IntegerType, DoubleType
from pytz import utc

from commons import domain
from job import MessagesIterable


class RandomMessagesIterable(MessagesIterable):
    def __iter__(self):
        return self

    def next(self):
        return generate()

    def __div__(self, concurrency_level=1):
        return map(lambda i: self, range(0, concurrency_level))


def generate():
    properties = map(lambda field: (field.name, _random(field.dataType)), domain.schema().fields)
    return dict(properties)


def _random(field_type):
    if isinstance(field_type, TimestampType):
        return random_datetime()
    elif isinstance(field_type, DateType):
        return random_date()
    elif isinstance(field_type, BooleanType):
        return random_boolean()
    elif isinstance(field_type, IntegerType):
        return random_positive_int()
    elif isinstance(field_type, DoubleType):
        random_float()
    else:
        raise ValueError('unsupported type')


def random_positive_int():
    return random.randint(0, 99999)


def random_boolean():
    return random.choice([True, False])


def random_float():
    return random.uniform(0.0, 99999.0)


DATE_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'


def random_datetime():
    return _random_datetime(DATE_TIME_FORMAT)


def _random_datetime(format_str):
    return datetime.fromtimestamp(random_positive_int(), tz=utc) \
        .strftime(format_str)


DATE_FORMAT = '%Y-%m-%d'


def random_date():
    return _random_datetime(DATE_FORMAT)
