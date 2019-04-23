from unittest import TestCase

import sys
from mock import Mock, patch

from consumer import arguments
from consumer.arguments import Arguments


class TestArguments(TestCase):

    def setUp(self):
        arguments._exit = Mock()
        sys.argv = ['two', 'arguments']
        arguments._MANDATORY_STREAMING_PROPERTIES = ['mandatory', 'config']
        arguments._read_config = Mock(return_value={'mandatory': 'value', 'config': 'value', 'extra': 'value'})
        arguments._app_name = Mock()

    @patch(target='sys.argv', new=['one_argument'])
    def test_init_should_exit_when_arguments_length_is_less_than_2(self):
        Arguments()
        arguments._exit.assert_called_once()

    @patch(target='consumer.arguments._read_config',
           new=Mock(return_value={'processingType': 'streaming', 'config': 'value'}))
    def test_init_should_exit_when_config_does_not_contain_all_mandatory_properties_for_given_processing_type(self):
        Arguments()
        arguments._exit.assert_called_once()

    def test_init_should_not_exit_when_2_arguments_given_and_config_contains_all_mandatory_properties(self):
        Arguments()
        assert not arguments._exit.called

    def test_processing_type_should_be_streaming_when_is_not_specified_in_config(self):
        arguments = Arguments()
        self.assertEqual('streaming', arguments.processing_type())
