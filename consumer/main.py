from pyspark.sql import SparkSession

from consumer.arguments import Arguments
from consumer.job import ConsumerJob

if __name__ == '__main__':
    arguments = Arguments()
    spark = SparkSession.builder \
        .getOrCreate()
    ConsumerJob(arguments, spark=spark).run()
