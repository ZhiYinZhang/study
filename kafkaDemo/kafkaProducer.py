#!/usr/bin/env python
# -*- coding:utf-8 -*-
from pykafka import KafkaClient
import time
from datetime import datetime
def kafka():
    client=KafkaClient(hosts="10.18.0.15:9193,10.18.0.19:9193,10.18.0.26:9193")
    # client=KafkaClient(hosts="dmpetl01.td.com:9092,dmpetl02.td.com:9092,dmpapp01.td.com:9092,dmpapp02.td.com:9092,dmp.td.com:9092,hadoop08.td.com:9092")
    topic=client.topics["test-log"]
    producer=topic.get_producer()
    i=0
    while 1:
        print( i)
        i += 1
        body='{"level": "INFO", "logger": "org.apache.coyote.http11.Http11NioProtocol","datetime":"%s+0800", "message": "Initializing ProtocolHandler ["http - nio - 8899"]","serverIp": "192.168.2.3"}'%time.strftime('%Y-%m-%d %H:%M:%S')
        producer.produce(body)
        # time.sleep(1)  9000000 约0.5s
        for i in range(5000000):
            pass


    # producer.stop()

if __name__=="__main__":
    kafka()
