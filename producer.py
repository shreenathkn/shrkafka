from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import sys

servid=sys.argv[1]
print('arg',servid)

producer = KafkaProducer(bootstrap_servers=['kafka-35faac3e-shreenath-7691.aivencloud.com:15964'],
security_protocol="SSL",
ssl_cafile="ca.pem",
ssl_certfile="service.cert",
ssl_keyfile="service.key",
value_serializer=lambda m: json.dumps(m).encode('ascii'))

data = producer.send('shrtest', {'ServiceId': servid,'details':{'status': 'active','stdate': '12-01-2019','enddate': '01-01-9999'}})
producer.metrics()
producer.flush()
try:
    record_metadata = data.get(timeout=1000)
    print('response Partition',record_metadata.partition)
    print ('response offset',record_metadata.offset)
except KafkaError:
    log.exception()    #Exception
