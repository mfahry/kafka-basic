#!/usr/bin/env python
#
# Copyright 2016 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Example Kafka Producer.
# Reads lines from stdin and sends to Kafka.
#

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
        #self.producer.produce(topic, params, callback=ProducerFunctions.delivery_callback)
        self.producer.produce('transaction', 'test-hallo')
        self.close_kafka()

    def transactionHandling(self, params):
        self.send_kafka(params, self.topics[0])
        return 'transaction'
	#self.send_kafka(params, self.topics[0])

    def transactionWithFraudHandling(self, params):
        self.send_kafka(params, self.topics[1])

    def close_kafka(self):
        self.producer.flush()
