#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
import json
import re
from pykafka import KafkaClient
import sys


def info():
    print("""
python2 order.py
--help
--add topicName batch   #add topic to ES.py,batch:每个批次写入ES多少条，默认200条
--delete topicName  #delete topoic from ES.py
--list              #list all topic in ES.py
""")

def readCheckpoint():
    """
    读取checkpoint文件中的topic
    :return: 返回从checkpoint文件中读取的topic
    """
    path='./checkpoint/topic.txt'
    topics = []
    if os.path.exists(path):
        with open(path, mode='r') as file:
            for i in json.load(file):
                topics.append(i['topic'])
        return topics
    else:
        print('no such file: %s'%path)
def getKafkaClient():
    kafka = KafkaClient(hosts='10.18.0.11:9092')
    return kafka
def isNum(num):
    try:
       num = int(num)
       if num>0:
           return True
       else:
           return False
    except Exception:
        print("TypeError:")
        return False
def isReally(param:dict):
    topic = param['topic']
    topics = readCheckpoint()
    kafkaClient = getKafkaClient()
    if re.search(r'.+(-log)$', topic):  # 是否符合规则
        if bytes(topic, 'utf-8') in kafkaClient.topics.keys():  # kafka中是否存在
            if topic not in topics:  # 是否已经在消费
                    batch = param.get("batch")
                    if batch:
                        if isNum(batch):
                            pass
                        else:
                            sys.exit(0)
                    producer = kafkaClient.topics[scheduler_topic].get_producer()
                    producer.produce(bytes(json.dumps(param), 'utf-8'))
                    producer.stop()
            else:
                print('warn : kafka topic %s is being comsumed' % topic)
        else:
            print('error: kafka topic "%s" not exist' % topic)
    else:
        print('error: kafka topic must end with "-log"')

if __name__=="__main__":
    if len(sys.argv) > 1:
        param = {}
        scheduler_topic = b'app'
        command = sys.argv[1]
        if command == '--help':
            info()
        elif command == '--list':
            topics = readCheckpoint()
            print(topics)

        elif command == '--add':
            param['command'] = 'add'
            if len(sys.argv) >= 3:
                topic = sys.argv[2]
                param['topic'] = topic
                if len(sys.argv) == 4:
                    batch = sys.argv[3]
                    param['batch'] = batch
                print(param)
                isReally(param)
            else:
                info()
        elif command == '--delete':
            param['command'] = 'delete'
            if len(sys.argv) == 3:
                topic = sys.argv[2]
                param['topic'] = topic
                print(param)
                if topic in readCheckpoint():
                    kafkaCli = getKafkaClient()
                    producer = kafkaCli.topics[scheduler_topic].get_producer()
                    producer.produce(bytes(json.dumps(param), 'utf-8'))
                    producer.stop()
                    print('exists')
                else:
                    print(f"topic '{topic}' is not exists")
            else:
                info()

        else:
            info()

    else:
        info()