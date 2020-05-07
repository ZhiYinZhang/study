#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/3/23 17:44
from pykafka import KafkaClient
from pykafka.topic import Topic
from datetime import datetime as dt
import time


client=KafkaClient("10.18.0.32:9092")

topic:Topic=client.topics[b"test1"]

#自定义分区器，producer.produce在生产消息时，传一个partition_key
def partitioner(pid,key):
    print(f"pid:{pid},key:{key}")
    #不管什么key，都发送到pid[0]分区
    return pid[0]

with topic.get_producer(partitioner=partitioner) as producer:
    for i in range(10):
        print(dt.now())
        producer.produce(f"test message: {i}".encode(),partition_key=b"a")
        time.sleep(2)

