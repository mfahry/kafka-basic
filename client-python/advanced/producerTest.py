from confluent_kafka import Producer
import json

data = '{"hallo" : "test1"}'
jsonData = json.loads(data)
p = Producer({'bootstrap.servers': '10.128.0.2:9092'})

p.produce('transaction', data)
p.flush()
