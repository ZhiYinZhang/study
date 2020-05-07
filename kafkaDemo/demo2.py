#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/4/28 10:00
from pykafka import KafkaClient
from pykafka.topic import Topic

client=KafkaClient("10.18.0.32:9092")

topic:Topic=client.topics[b"test"]

consumer=topic.get_simple_consumer(b"g1")
for message in consumer:
    print(consumer.held_offsets)
    pid=message.partition_id
    data=message.value
    offset=message.offset
    print(f"partition_id:{pid} offset:{offset}  message:{data}")
    consumer.commit_offsets()