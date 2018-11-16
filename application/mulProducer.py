#!/usr/bin/env python
# -*- coding:utf-8 -*-
from pykafka import KafkaClient
import random
import time
import threading
import json



class myThread(threading.Thread):
    def __init__(self,topic,kafkaClient):
        threading.Thread.__init__(self)
        self.topic=topic
        self.kafkaClient=kafkaClient
    def run(self):
        print('start--------%s'%self.topic)
        start(topic=self.topic,kafkaClient=self.kafkaClient)
        print('stop---------%s'%self.topic)

def start(topic,kafkaClient):
    l = ['error', 'debug', 'info', 'warn']
    producer = kafkaClient.topics[bytes(topic,'utf-8')].get_producer()
    for n in range(10000000000):
        # time.sleep(1)
        print(topic)
        level = l[random.randrange(0, l.__len__())]
        body = '{"level": "%s", "logger": "org.apache.coyote.http11.Http11NioProtocol","datetime":"%s+0800", "message": "Initializing ProtocolHandler [http - nio - 8899]","serverIp": "192.168.2.3"}' % (
        level, time.strftime(
            '%Y-%m-%d %H:%M:%S'))
        producer.produce(bytes(body,'utf-8'))
    producer.stop()

# client=KafkaClient(hosts='119.29.165.154:9092')

client = KafkaClient(hosts='10.18.0.11:9092,10.18.0.8:9092')
topics=['test1-log']

if __name__=="__main__":
     for topic in topics:
         myThread(topic=topic,kafkaClient=client).start()