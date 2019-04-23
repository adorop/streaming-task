import json
import sys
from multiprocessing.dummy import Pool

from kafka import KafkaProducer

from job import PublisherJob, WorkersPool, CloseableKafkaProducer
from random_messages import RandomMessagesIterable

_1_Mb = 1048576


class ThreadPool(WorkersPool):

    def __init__(self, processes):
        self.delegatee = Pool(processes=processes)

    def submit(self, f, **kwargs):
        self.delegatee.apply_async(f, kwds=kwargs)

    def await(self):
        self.delegatee.close()
        self.delegatee.join()


class CloseableKafkaProducerAdapter(CloseableKafkaProducer):

    def __init__(self, **configs):
        self.adaptee = KafkaProducer(**configs)

    def send(self, topic, value, key=None):
        self.adaptee.send(topic, value, key)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.adaptee.close()


def job_parameters():
    if len(sys.argv) < 4:
        sys.stderr.write('expected arguments are: concurrency level, kafka topic, kafka bootstrap servers')
        sys.exit(-1)

    return int(sys.argv[1]), sys.argv[2], sys.argv[3]


if __name__ == '__main__':
    concurrency_level, topic, bootstrap_servers = job_parameters()
    batch_size = _1_Mb
    buffer_size = batch_size * concurrency_level
    producer = CloseableKafkaProducerAdapter(bootstrap_servers=bootstrap_servers,
                                             value_serializer=lambda data: json.dumps(data),
                                             acks=0,
                                             batch_size=batch_size,
                                             buffer_memory=buffer_size)
    PublisherJob(
        ThreadPool(processes=concurrency_level),
        RandomMessagesIterable(),
        topic,
        producer,
        concurrency_level
    ).run()
