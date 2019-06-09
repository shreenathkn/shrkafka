from kafka import KafkaConsumer
import json
from psycopg2.extras import RealDictCursor
import psycopg2
# To consume latest messages and auto-commit offsets

consumer = KafkaConsumer('shrtest',
                         group_id='shr-group',
                         auto_offset_reset="latest",
                         bootstrap_servers=['kafka:port'],
                         security_protocol="SSL",
                         ssl_cafile="ca.pem",
                         ssl_certfile="service.cert",
                         ssl_keyfile="service.key",
                         consumer_timeout_ms=10000,
                         value_deserializer=lambda m: json.loads(m.decode('ascii')))

for message in consumer:
    #message = message.value;
    servicedet=message.value
    #serviceid = message.ServiceId
    print('serviceid recvd ',servicedet['ServiceId'])
    print('details recvd ',servicedet['details'])

    consumer.commit()
    consumer.metrics()
    #Store the result in PostGreSql
    try:
        uri = "postgres://"
        db_conn = psycopg2.connect(uri)
        c = db_conn.cursor(cursor_factory=RealDictCursor)
        inputval = servicedet['details'];#{'status':'active','stdate':'12-01-2019','enddate':'01-01-9999'}
        c.execute("INSERT INTO service_details(ServiceId,details) VALUES (%s, %s)", (servicedet['ServiceId'],json.dumps(inputval)))
        db_conn.commit()
        count = c.rowcount
        print (count, "Record inserted successfully into service_details table")

        #Fetch results
        sql_select_query = """SELECT * FROM service_details where ServiceId=%s"""
        c.execute(sql_select_query, (servicedet['ServiceId'],))
        record = c.fetchone()
        print('resul select',record)
        db_conn.commit()
    except (Exception, psycopg2.Error) as error :
        if(db_conn):
            print("Failed DB error", error)
    finally:
        #closing database connection.
        if(db_conn):
            c.close()
            #selCursor.close()
            db_conn.close()
            print("PostgreSQL connection is closed")
