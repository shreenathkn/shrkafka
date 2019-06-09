# shrkafka

The code has two python programs producer and consumer.

# Producer:
The producer python script accepts and id as the parameter and is used as a key while sending data to the topic.
Usage: python producer.py 34566.
This publishes a json record to topic 'shrtest' hosted on Aiven Kafka Service.
The record format is : {'ServiceId': 34566,'details':{'status': 'active','stdate': '12-01-2019','enddate': '01-01-9999'}}

This script can be enhanced to accept values for the inner json (details) as well.

# Consumer
The consumer script fetches the json record from the topic and stores it in the service_details table under Aiven PostgreSql DB.This script also queries the recently inserted data so that insertion can be verified.
Usage:python consumer.py

1 Record inserted successfully into service_details table
resul select RealDictRow([('serviceid', '34566'), ('details', {'status': 'active', 'stdate': '12-01-2019', 'enddate': '01-01-9999'})])



# Testing considerations
By querying the Postgresql for the relevant service id the scripts can be verified.



