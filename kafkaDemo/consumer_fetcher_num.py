#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/3/18 10:29
from pykafka import KafkaClient
from pykafka.topic import Topic
from pykafka.partition import Partition
from pykafka.protocol import Message
import time

client=KafkaClient("10.18.0.32:9092")

topic:Topic=client.topics[b"test"]

#获取消费者，并设置消费者组
consumer=topic.get_simple_consumer(consumer_id=b"r1",
                                   auto_commit_enable=True,
                                   auto_commit_interval_ms=100,
                                   num_consumer_fetchers=2  #设置多个拉取线程，默认:1
                                   )
values=[]
start_time=time.time()
while True:
    message:Message=consumer.consume()

    value = message.value
    pid = message.partition_id
    offset=message.offset

    # print(f"partition_id:{pid} offset:{offset}  message:{value}")
    values.append(value)
    end_time=time.time()
    if end_time-start_time>=10:
        print(f"[{start_time} - {end_time}],values:{len(values)}")
        values=[]
        start_time=time.time()



