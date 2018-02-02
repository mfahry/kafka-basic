from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro.serializer import (KeySerializerError,ValueSerializerError)
from tests.avro.mock_schema_registry_client import MockSchemaRegistryClient

class ProducerFunctions:
    broker = '172.18.133.252:9092'
    topics = ("transaction", "transactionWithFraud")

    def __init__(self):
        conf = {'bootstrap.servers': ProducerFunctions.broker}
        self.producer = Producer(**conf)
        value_schema = avro.load("schema.avsc")
        producer = AvroProducer({'schema.registry.url': 'http://172.18.133.252:9001'}, default_value_schema=value_schema)

    def delivery_callback(self, err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %o\n' % (msg.topic(), msg.partition(), msg.offset()))

    def send_kafka(self, value, topic):
        producer.produce(topic=topic, value=value)
        self.close_kafka()

    def close_kafka(self):
        self.producer.flush()
