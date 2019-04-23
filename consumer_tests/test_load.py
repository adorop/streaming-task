from unittest import TestCase

from mock import Mock, call

from consumer import OUTPUT_LOCATION_OPTION
from consumer.load import StreamLoad, CHECKPOINT_LOCATION_OPTION, BatchWriteToCsvLoad

EXPECTED_CHECKPOINT_DIR = 'dir'
EXPECTED_ARGUMENTS_OUTPUT_PATH = '/path'
EXPECTED_SINK = 'sink'


class TestStreamLoad(TestCase):
    def setUp(self):
        self.init_arguments()
        self.input = Mock()
        self.init_writer()
        self.init_query()
        self.load = StreamLoad(self.arguments)

    def init_arguments(self):
        self.arguments = Mock()
        self.arguments.checkpoint_dir = Mock(return_value=EXPECTED_CHECKPOINT_DIR)
        self.arguments.output_path = Mock(return_value=EXPECTED_ARGUMENTS_OUTPUT_PATH)
        self.arguments.sink = Mock(return_value=EXPECTED_SINK)

    def init_writer(self):
        self.writer = Mock()
        self.input.writeStream = self.writer
        self.writer.option = Mock(return_value=self.writer)
        self.writer.format = Mock(return_value=self.writer)

    def init_query(self):
        self.query = Mock()
        self.writer.start = Mock(return_value=self.query)
        self.query.awaitTermination = Mock()

    def test_should_set_checkpoint_and_output_location_options(self):
        self.load(self.input)
        self.writer.assert_has_calls(self.set_checkpoint_and_path_location_options_calls())

    def set_checkpoint_and_path_location_options_calls(self):
        return [
            call.option(CHECKPOINT_LOCATION_OPTION, EXPECTED_CHECKPOINT_DIR),
            call.option(OUTPUT_LOCATION_OPTION, EXPECTED_ARGUMENTS_OUTPUT_PATH)
        ]

    def test_should_writer_format_to_arguments_sink(self):
        self.load(self.input)
        self.writer.format.assert_called_with(EXPECTED_SINK)

    def test_should_start_load(self):
        self.load(self.input)
        self.writer.start.assert_called()

    def test_should_await_termination(self):
        self.load(self.input)
        self.query.awaitTermination.assert_called()


class TestBatchWriteToCsvLoad(TestCase):

    def setUp(self):
        self.init_arguments()
        self.input = Mock()
        self.init_writer()
        self.load = BatchWriteToCsvLoad(self.arguments)

    def init_arguments(self):
        self.arguments = Mock()
        self.arguments.output_path = Mock(return_value=EXPECTED_ARGUMENTS_OUTPUT_PATH)

    def init_writer(self):
        self.writer = Mock()
        self.input.write = self.writer
        self.writer.csv = Mock()

    def test_should_set_csv_path_to_arguments_output_path(self):
        self.load(self.input)
        self.writer.csv.assert_called_once_with(EXPECTED_ARGUMENTS_OUTPUT_PATH)
