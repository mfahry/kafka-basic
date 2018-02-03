
from confluent_kafka import Consumer, KafkaError
import json, requests

c = Consumer({'bootstrap.servers': '10.128.0.2:9092', 'group.id': 'mygroup', 'default.topic.config': {'auto.offset.reset': 'smallest'}})
c.subscribe(['transaction'])
running = True
while running:
    msg = c.poll()

    if not msg.error():
        jsonString = msg.value().decode('utf-8').replace("'",'"')
        jsonData =  json.loads(jsonString)
        print(jsonData)

        # change to FDS microservices
        # url = 'http://localhost:8080'
        # data = open('example.json','r')
        # headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        # r = requests.post(url, data=json.dumps(data), headers=headers)

    elif msg.error().code() != KafkaError._PARTITION_EOF:
        print(msg.error())
        running = False
c.close()
