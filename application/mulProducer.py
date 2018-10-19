#!/usr/bin/env python
# -*- coding:utf-8 -*-
from pykafka import KafkaClient
import random
import time
import threading
# client=KafkaClient(hosts='119.29.165.154:9092')
# client=KafkaClient(hosts='10.18.0.15:9092')
# print client.topics


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
    producer = kafkaClient.topics[topic].get_producer()
    for n in range(1000000):
        print(topic)
        level = l[random.randrange(0, l.__len__())]
        body = '{"level": "%s", "logger": "org.apache.coyote.http11.Http11NioProtocol","datetime":"%s+0800", "message": "Initializing ProtocolHandler [http - nio - 8899]","serverIp": "192.168.2.3"}' % (
        level, time.strftime(
            '%Y-%m-%d %H:%M:%S'))
        body1 = '{"level": "%s", "logger": "org.apache.coyote.http11.Http11NioProtocol","datetime":"%s+0800", "message": "Initializing ProtocolHandler ["http - nio - 8899"]","serverIp": "192.168.2.3"}' % (
            level, time.strftime('%Y-%m-%d %H:%M:%S'))
        producer.produce(body)
        producer.produce(body1)
    producer.stop()
client = KafkaClient(hosts='10.18.0.15:9193,10.18.0.19:9193')
topics=['test-log','test1-log','test2-log','test3-log']

# client = KafkaClient(hosts='10.18.0.15:9193')
# for topic in topics:
#     print('start--------%s'%topic)
#     l=['error','debug','info','warn']
#     producer=client.topics[topic].get_producer()
#     for n in range(1000000):
#         level=l[random.randrange(0,l.__len__())]
#         body = '{"level": "%s", "logger": "org.apache.coyote.http11.Http11NioProtocol","datetime":"%s+0800", "message": "Initializing ProtocolHandler [http - nio - 8899]","serverIp": "192.168.2.3"}\n' %(level,time.strftime(
#                 '%Y-%m-%d %H:%M:%S'))
#         body1 = '{"level": "%s", "logger": "org.apache.coyote.http11.Http11NioProtocol","datetime":"%s+0800", "message": "Initializing ProtocolHandler ["http - nio - 8899"]","serverIp": "192.168.2.3"}\n' % (
#         level, time.strftime('%Y-%m-%d %H:%M:%S'))
#         producer.produce(body)
#         producer.produce(body1)
#     producer.stop()
#     print('stop--------%s'%topic)


for topic in topics:
    myThread(topic=topic,kafkaClient=client).start()