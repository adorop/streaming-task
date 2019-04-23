from unittest import TestCase

from mock import Mock, patch

from consumer import job
from consumer.job import ConsumerJobTemplate
from consumer.load import BatchWriteToCsvLoad, StreamLoad

EXTRACTED = 'extracted'
TRANSFORMED = 'transformed'


class TestConsumerJobTemplate(TestCase):
    def setUp(self):
        self.extract = Mock(return_value=EXTRACTED)
        self.transform = Mock(return_value=TRANSFORMED)
        self.load = Mock()
        self.job = ConsumerJobTemplate(self.extract, self.transform, self.load)

    def test_should_load_extracted_transformed_data(self):
        self.job.run()
        self.extract.assert_called_once()
        self.transform.assert_called_once_with(EXTRACTED)
        self.load.assert_called_once_with(TRANSFORMED)


BATCH_READER = 'batchReader'
STREAM_READER = 'streamReader'


@patch(target='consumer.load.StreamLoad.__init__', new=Mock(return_value=None))
@patch(target='consumer.load.BatchWriteToCsvLoad.__init__', new=Mock(return_value=None))
class TestConsumerJobFactory(TestCase):

    def setUp(self):
        self.spark = Mock()
        self.init_readers()
        self.init_arguments()

    def init_readers(self):
        self.spark.readStream = STREAM_READER
        self.spark.read = BATCH_READER

    def init_arguments(self):
        self.arguments = Mock()

    def test_should_return_stream_reader_and_stream_loader_when_processing_type_is_streaming(self):
        self.arguments.processing_type = Mock(return_value='streaming')
        reader, load = job._get_reader_and_load(self.spark, self.arguments)
        self.assertEqual(STREAM_READER, reader)
        self.assertIsInstance(load, StreamLoad)

    def test_should_return_batch_reader_and_batch_loader_when_processing_type_is_batch(self):
        self.arguments.processing_type = Mock(return_value='batch')
        reader, load = job._get_reader_and_load(self.spark, self.arguments)
        self.assertEqual(BATCH_READER, reader)
        self.assertIsInstance(load, BatchWriteToCsvLoad)
