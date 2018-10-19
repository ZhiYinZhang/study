#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
from pykafka import KafkaClient
import sys


def info():
    print("""
python2 order.py
--help
--add topicName     #add topic to ES.py
--delete topicName  #delete topoic from ES.py
--list              #list all topic in ES.py
""")

def readCheckpoint():
    """
    读取checkpoint文件中的topic
    :return: 返回从checkpoint文件中读取的topic
    """
    path='./checkpoint/topic.txt'
    if os.path.exists(path):
        with open(path, mode='r') as file:
            print (file.read())
    else:
        print('no such file: %s'%path)
def getProducer():
    kafka = KafkaClient(hosts='10.18.0.15:9193,10.18.0.19:9193')
    producer = kafka.topics['rate'].get_producer()
    return producer

if len(sys.argv) > 1:

    command = sys.argv[1]
    if command == '--help':
        info()
    elif command == '--list':
        print(readCheckpoint())
    elif command == '--add':
        if len(sys.argv) == 3:
            topic = sys.argv[2]
            producer=getProducer()
            producer.produce(topic)
            producer.stop()
        else:
            info()
    elif command == '--delete':
        if len(sys.argv) == 3:
            topic = sys.argv[2]
            producer = getProducer()
            producer.produce('%s_delete' % topic)
            producer.stop()
        else:
            info()

    else:
        info()

else:
    info()