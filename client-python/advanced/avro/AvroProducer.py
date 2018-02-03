from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

value_schema = avro.load('ValueSchema.avsc')
key_schema = avro.load('KeySchema.avsc')
value = { \
    	"nokartu":"12345678909876543", \
    	"amounttransaction":"10000.00", \
    	"terminaltype":"ATM BRI", \
    	"terminalid":"0001", \
    	"terminalinformation":"Uji Coba", \
    	"statustransaction":"00 - Sukses", \
    	"transdate":"2018-01-01", \
    	"transtime":"14:30:00", \
    	"descriptiontranction":"AD" \
    }

key = { \
        "nokartu":"12345678909876543", \
        "amounttransaction":"10000.00", \
        "terminaltype":"ATM BRI", \
        "terminalid":"0001", \
        "terminalinformation":"Uji Coba", \
        "statustransaction":"00 - Sukses", \
        "transdate":"2018-01-01", \
        "transtime":"14:30:00", \
        "descriptiontranction":"AD" \
    }

avroProducer = AvroProducer({'bootstrap.servers': '10.128.0.4:9092', 'schema.registry.url': 'http://localhost:8081'}, default_key_schema=key_schema, default_value_schema=value_schema)
avroProducer.produce(topic='my_topic', value=value, key=key)
avroProducer.flush()
