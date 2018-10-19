#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
import subprocess as sp
import sys
import pykafka
def info():
    print ('--start')
    print ('--stop')
    print ('--list')
    print ('--delete topic')
    print ('--create partitions repli topic')
    print ('--produce topic message')

home='/home/zhangzy/kafka_2.11-1.1.0'
zk='10.18.0.15:3191,10.18.0.19:3191,10.18.0.26:3191'
kafkaHost='10.18.0.15:9193,10.18.0.19:9193,10.18.0.26:9193'

def producer(kafkaHost,topic,message):
    kafka=pykafka.KafkaClient(kafkaHost)
    if kafka.topics.has_key(topic):
       producer=kafka.topics[topic].get_producer()
       producer.produce(message=message)
       producer.stop()
    else:
        print("kafka topic is not exists")
if sys.argv.__len__()>1:
    global param
    param=sys.argv[1]

    if param=='--help':
         info()
    elif param=='--start':
        sp.call('nohup %s/bin/kafka-server-start.sh %s/config/server.properties >/dev/null 2>&1 &'%(home,home),shell=True)
    elif param=='--stop':
        str = sp.check_output('jps', shell=True)#获取进程
        for temp in str.split('\n'):#以换行符切割
            if 'Kafka' in temp:#获取含有kafka进程名的字符串
                kafka = temp.split(' ')[0]#以空格切割获取kafka进程id
                sp.call('kill -9 %s'%kafka,shell=True)
    elif param=='--list':
        sp.call('%s/bin/kafka-topics.sh --list --zookeeper %s'%(home,zk),shell=True)
    elif param=='--delete':
        if sys.argv.__len__()==3:
            topic=sys.argv[2]
            sp.call('%s/bin/kafka-topics.sh --delete --zookeeper %s --topic %s'%(home,zk,topic),shell=True)
        else:
            info()
    elif param=='--create':
        if sys.argv.__len__()==5:
            partitions=sys.argv[2]
            repli=sys.argv[3]
            topic=sys.argv[4]
            sp.call('%s/kafka-topics.sh --create --zookeeper %s \
    --partitions %s --replication-factor %s --topic %s'%(home,zk,partitions,repli,topic),shell=True)
        else:
            info()
    elif param=='--produce':
        if sys.argv.__len__()==4:
            topic=sys.argv[2]
            message=sys.argv[3]
            producer(kafkaHost=kafkaHost,topic=topic,message=message)
        else:
            info()
    else:
        info()
else:
    info()
