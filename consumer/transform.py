from pyspark.sql.functions import from_json, current_timestamp


class ParseJsonColumnTransform(object):

    def __init__(self, json_column_name, schema):
        self.json_column_name = json_column_name
        self.schema = schema

    def __call__(self, *args, **kwargs):
        extracted_data_frame = args[0]
        return extracted_data_frame \
            .select(from_json(self.json_column_name, self.schema).alias(self.json_column_name)) \
            .select(self._all_properties())

    def _all_properties(self):
        return '{}.*'.format(self.json_column_name)


class AddTimestampColumnDecoratorTransform(object):

    def __init__(self, delegatee, timestamp_field_name):
        self.delegatee = delegatee
        self.timestamp_field_name = timestamp_field_name

    def __call__(self, *args, **kwargs):
        return self.delegatee(*args, **kwargs) \
            .withColumn(self.timestamp_field_name, current_timestamp())
