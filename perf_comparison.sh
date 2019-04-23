#!/usr/bin/env bash

topic='perf_comparision_topic'
streaming_app='performance_comparison_streaming'
batch_app='performance_comparison_batch'
time_to_run='30m'

submit() {
    app_name=$1
    config_file=$2
    spark-submit --master yarn \
     --name ${app_name} \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 \
     --py-files target/consumer.zip \
     target/main.py  ${config_file} &> consumer_log &
}


kill_app() {
    app_id=$( yarn application -list -appStates RUNNING | grep $1 | awk -F' ' '{print $1}' | tail -n 1 )
    yarn application -kill ${app_id}
}

export CHECK_POINTING_DIR=hdfs://sandbox-hdp.hortonworks.com/user/aliaksei/chckpnt && export KAFKA_BOOTSTRAP_SERVERS=sandbox-hdp.hortonworks.com:6667

submit ${streaming_app} '/root/stream_config.json'
sleep ${time_to_run}
kill_app ${streaming_app}


submit ${batch_app} '/root/batch_config.json'
sleep ${time_to_run}


hadoop fs -du -s -h /user/aliaksei/performance_comparison/batch
hadoop fs -du -s -h /user/aliaksei/performance_comparison/stream

kill_app ${batch_app}


