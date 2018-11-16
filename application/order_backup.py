# datetime:2018/11/15 15:37
#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
import json
import re
from pykafka import KafkaClient
import argparse



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

def add(param:dict):
    topic = param['topic']
    topics = readCheckpoint()
    kafkaClient = getKafkaClient()
    if re.search(r'.+(-log)$', topic):  # 是否符合规则
        if bytes(topic, 'utf-8') in kafkaClient.topics.keys():  # kafka中是否存在
            if topic not in topics:  # 是否已经在消费

                    producer = kafkaClient.topics[scheduler_topic].get_producer()
                    producer.produce(bytes(json.dumps(param), 'utf-8'))
                    producer.stop()
            else:
                print('warn : kafka topic %s is being comsumed' % topic)
        else:
            print('error: kafka topic "%s" not exist' % topic)
    else:
        print('error: kafka topic must end with "-log"')
def delete(param:dict):
    topic = param['topic']
    if topic in readCheckpoint():

        kafkaCli = getKafkaClient()
        producer = kafkaCli.topics[scheduler_topic].get_producer()
        producer.produce(bytes(json.dumps(param), 'utf-8'))
        producer.stop()
    else:
        print(f"topic '{topic}' is not exists")


if __name__=="__main__":
    parser = argparse.ArgumentParser()

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--list', '-l', dest='command', nargs='?', const='query', help='列出所有正在消费的kafka topic')
    group.add_argument('--add', '-a', dest='command', nargs='?', const='add', help="添加一个topic去消费")
    group.add_argument('--delete', '-d', dest='command', nargs='?', const='delete', help='从正在消费的topics中移除')

    parser.add_argument('--topic', '-t', type=str, nargs='?', help="kafka topic name")

    add_parser = parser.add_argument_group("optional of add operate")
    add_parser.add_argument('--batch', '-b', type=int, nargs='?', const=200, default=200,
                     help="写入到Elasticsearch每个批次的大小，batch越大，速度越快，实时性越低，默认即可,default:200")
    add_parser.add_argument('--retain', '-r', type=int, nargs='?', const=2, default=2,
                     help="删除Elasticsearch过期index时,保留多少个(包括正在写的),default:2")
    add_parser.add_argument('--interval', '-i', type=int, nargs='?', const=1, default=1,
                     help="Elasticsearch每隔多久生成topic的一个新的index,即日志每隔多久rollover一次,default:1")
    add_parser.add_argument('--unit', '-u', type=str, choices=('d', 'h', 'm', 's'), nargs='?', const='d', default='d',
                     help="设置interval单位,d:day h:hour m:minutes s:second,default:d")

    args = parser.parse_args()


    command = args.command
    param = {}
    scheduler_topic = b'app'
    topic = args.topic
    if command == 'query':
        topics = readCheckpoint()
        print(topics)
    elif command == 'add':
        if topic:
           param['command'] = command
           param['topic'] = topic
           param['batch'] = args.batch
           param['retain'] = args.retain
           param['interval'] = args.interval
           param['unit'] = args.unit
           print(f"param:{param}")
           add(param)
        else:
            parser.print_help()
            print("ValueError:in '--add'/'--delete' operate,the '--topic' parameter must be set")
    elif command == 'delete':
        if topic:
            param['command'] = command
            param['topic'] = topic
            print(f"param:{param}")
            delete(param)
        else:
            parser.print_help()
            print("ValueError:in '--add'/'--delete' operate,the '--topic' parameter must be set")



