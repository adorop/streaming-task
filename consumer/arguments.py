import json
import sys

from pyspark.sql import SparkSession

from consumer import CSV_OUTPUT_FORMAT

_MANDATORY_BATCH_PROPERTIES = ('kafkaBootstrapServers', 'kafkaTopic', 'outputPath')
_MANDATORY_STREAMING_PROPERTIES = _MANDATORY_BATCH_PROPERTIES + ('checkpointingDir',)


class Arguments(object):

    def __init__(self):
        if len(sys.argv) < 2:
            _exit('path to configuration json file is required')
            return
        self.config = _read_config()
        self._processing_type = self.config.get('processingType', 'streaming')
        if not self._all_mandatory_properties_provided():
            _exit('{} are required for {} processing type'.format(self._mandatory_properties(), self._processing_type))
        self.app_name = _app_name()

    def _all_mandatory_properties_provided(self):
        for p in self._mandatory_properties():
            if p not in self.config:
                return False
        return True

    def _mandatory_properties(self):
        if self._processing_type == 'streaming':
            return _MANDATORY_STREAMING_PROPERTIES
        elif self._processing_type == 'batch':
            return _MANDATORY_BATCH_PROPERTIES
        else:
            _exit('unknown processing type')

    def processing_type(self):
        return self._processing_type

    def kafka_bootstrap_servers(self):
        return self.config['kafkaBootstrapServers']

    def kafka_topic(self):
        return self.config['kafkaTopic']

    def output_path(self):
        return self.config['outputPath']

    def checkpoint_dir(self):
        return '{}/{}'.format(self.config['checkpointingDir'], self.app_name)

    def sink(self):
        return self.config.get('sink', CSV_OUTPUT_FORMAT)


def _exit(error_message):
    sys.stderr.write(error_message)
    sys.exit(-1)


def _read_config():
    with open(sys.argv[1]) as f:
        return json.load(f)


def _app_name():
    return SparkSession.builder \
        .getOrCreate() \
        .sparkContext.appName
