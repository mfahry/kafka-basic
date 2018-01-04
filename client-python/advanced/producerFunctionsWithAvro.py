from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro.serializer import (KeySerializerError,ValueSerializerError)
from tests.avro.mock_schema_registry_client import MockSchemaRegistryClient
from confluent_kafka import Producer

class ProducerFunctions:
    broker = '10.128.0.2:9092'
    topics = ("transaction", "transactionWithFraud")

    def __init__(self):
        conf = {'bootstrap.servers': ProducerFunctions.broker}
        self.producer = Producer(**conf)

    def delivery_callback(self, err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %o\n' % (msg.topic(), msg.partition(), msg.offset()))

    def send_kafka(self, params, topic):
        value_schema = avro.load("schema.avsc"))
        producer = AvroProducer({'schema.registry.url': 'http://10.128.0.2:9001'}, default_value_schema=value_schema)
        producer.produce(topic=topic, value=params)
        self.close_kafka()

    def transactionHandling(self, params):
        self.send_kafka(params, self.topics[0])

    def transactionWithFraudHandling(self, params):
        self.send_kafka(params, self.topics[1])

    def close_kafka(self):
        self.producer.flush()
