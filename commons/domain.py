from pyspark.sql.types import StructField, IntegerType, DateType, DoubleType, TimestampType, StructType, BooleanType


def schema():
    return StructType(
        [
            StructField('date_time', TimestampType()),
            StructField('site_id', IntegerType()),
            StructField('site_continent_id', IntegerType()),
            StructField('user_location_country_id', IntegerType()),
            StructField('user_location_region_id', IntegerType()),
            StructField('user_location_city_id', IntegerType()),
            StructField('orig_destination_distance', DoubleType()),
            StructField('user_id', IntegerType()),
            StructField('is_mobile', BooleanType()),
            StructField('is_package', BooleanType()),
            StructField('channel_id', IntegerType()),
            StructField('search_check_in', DateType()),
            StructField('search_check_out', DateType()),
            StructField('search_adults_count', IntegerType()),
            StructField('search_children_count', IntegerType()),
            StructField('search_rooms_count', IntegerType()),
            StructField('search_destination_id', IntegerType()),
            StructField('search_destination_type_id', IntegerType()),
            StructField('is_booking', BooleanType()),
            StructField('similar_events_count', IntegerType()),
            StructField('hotel_continent_id', IntegerType()),
            StructField('hotel_country_id', IntegerType()),
            StructField('hotel_market_id', IntegerType()),
            StructField('hotel_cluster', IntegerType())
        ]
    )
