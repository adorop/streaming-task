from consumer import OUTPUT_LOCATION_OPTION, CSV_OUTPUT_FORMAT

CHECKPOINT_LOCATION_OPTION = 'checkpointLocation'


class StreamLoad(object):

    def __init__(self, arguments):
        self.arguments = arguments

    def __call__(self, *args, **kwargs):
        transformed = args[0]
        transformed.writeStream \
            .option(CHECKPOINT_LOCATION_OPTION, self.arguments.checkpoint_dir()) \
            .option(OUTPUT_LOCATION_OPTION, self.arguments.output_path()) \
            .format(self.arguments.sink()) \
            .start() \
            .awaitTermination()


class BatchWriteToCsvLoad(object):

    def __init__(self, arguments):
        self.arguments = arguments

    def __call__(self, *args, **kwargs):
        transformed = args[0]
        transformed.write \
            .csv(self.arguments.output_path())
