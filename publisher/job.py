from abc import ABCMeta, abstractmethod


class PublisherJob(object):

    def __init__(self,
                 workers_pool,
                 messages_to_publish,
                 kafka_topic,
                 closeable_kafka_producer,
                 workers_count):
        self.workers_pool = workers_pool
        self.messages_to_publish = messages_to_publish
        self.kafka_topic = kafka_topic
        self.concurrency_level = workers_count
        self.closeable_kafka_producer = closeable_kafka_producer

    def run(self):
        with self.closeable_kafka_producer:
            for one_task_messages in self.messages_to_publish / self.concurrency_level:
                task = Task(one_task_messages, self.kafka_topic, self.closeable_kafka_producer)
                self.workers_pool.submit(task)
            self.workers_pool.await()


class WorkersPool(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def submit(self, f, **kwargs):
        pass

    @abstractmethod
    def await(self):
        pass


class MessagesIterable(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def __iter__(self):
        pass

    @abstractmethod
    def next(self):
        pass

    def __div__(self, concurrency_level=1):
        pass


class Task(object):

    def __init__(self, messages, topic, kafka_producer):
        self.messages = messages
        self.topic = topic
        self.closeable_kafka_producer = kafka_producer

    def __call__(self, *args, **kwargs):
        for message in self.messages:
            self.closeable_kafka_producer.send(self.topic, value=message)

    def __eq__(self, o):
        return self.__dict__ == o.__dict__

    def __hash__(self):
        return hash(frozenset(self.__dict__.iteritems()))


class CloseableKafkaProducer(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def send(self, topic, value, key=None):
        pass

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
