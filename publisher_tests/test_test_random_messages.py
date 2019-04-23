import unittest

import collections
from mock import Mock

from publisher import random_messages
from publisher.random_messages import RandomMessagesIterable

GENERATED_MESSAGE = 'generated_message'


class TestRandomMessagesIterable(unittest.TestCase):
    def setUp(self):
        random_messages.generate = Mock(return_value=GENERATED_MESSAGE)
        self.iterable = RandomMessagesIterable()

    def test_next_should_return_generated_message(self):
        for message in self.iterable:
            self.assertEqual(message, GENERATED_MESSAGE)
            break

    def test_div_should_return_given_number_of_self_instances(self):
        divisor = 3
        result = self.iterable / divisor
        self.assertEqual(len(result), divisor)
        self.assertRandomMessagesIterableInstances(result)

    def assertRandomMessagesIterableInstances(self, result):
        for item in result:
            self.assertEqual(item, self.iterable)


EXPECTED_GENERATED_MESSAGE_LENGTH = 24


class TestMessageGenerator(unittest.TestCase):
    def test_generate_should_return_dict_of_24_elements(self):
        result = random_messages.generate()
        self.assertIsInstance(result, collections.Mapping)
        self.assertEqual(len(result), EXPECTED_GENERATED_MESSAGE_LENGTH)


class TestRandoms(unittest.TestCase):

    def setUp(self):
        random_messages.random_positive_int = Mock(return_value=0)

    def test_random_date_time_should_return_string_in_format_YYYY_MM_DD_HH_mm_ss(self):
        result = random_messages.random_datetime()
        self.assertEqual('1970-01-01 00:00:00', result)

    def test_random_date_should_return_string_in_format_YYYY_MM_DD(self):
        result = random_messages.random_date()
        self.assertEqual('1970-01-01', result)
