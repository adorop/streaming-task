# Streaming Basics Task

## Description

Consists of 2 main modules:

* publisher: sends random booking events to Kafka topic in parallel
* consumer: Spark job that reads Kafka messages *values* and loads them to HDFS in csv format 

## Dependencies

Required third-party libraries are listed in [requirements](requirements.txt) file and can be installed  
using `pip install -r requirements.txt`

## Build

Ships as Python project, thus, does not require compilation itself. 

However, when submitting *consumer* application to Spark, [main.py](consumer/main.py) file is passed separately from its dependencies

[Makefile](Makefile) can simplify this process. It:

* cleans up **.py* files
* runs tests
* packages [main.py](consumer/main.py) and zipped dependencies in *target/* directory

Use
```
make clean build
```

##Run

###Publisher
Requires 3 arguments:
* Concurrency level, i.e. number of threads publishing messages to Kafka in parallel
* Kafka topic
* Kafka bootstrap servers

```
$ python publisher/main.py 3 my_topic kafka_host:${KAFKA_PORT}
```

###Consumer
Requires path to json config file
 ```
 $ spark-submit --master yarn \ 
  --name consumer \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 \
  --py-files target/consumer.zip \
  target/main.py /path/to/config.json
 ```
Example configuration file:
```json
{
  "processingType": "streaming|batch" //defaults to streaming
  "kafkaBootstrapServers": "kafka_host:kafka_port", 
  "kafkaTopic": "example_topic", 
  "outputPath": "hdfs://hdfs_host/path"
}
```




#Testing
Both main modules have unit tests suites.
[test_integration.py](consumer_tests/test_integration.py) contains Spark-related integration tests for consumer.
```
$ python -m unittest discover
```