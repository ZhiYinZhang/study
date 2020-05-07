#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/3/18 10:29
from pykafka import KafkaClient
from pykafka.topic import Topic
from pykafka.protocol import Message

client=KafkaClient("10.18.0.32:9092")

topic:Topic=client.topics[b"test"]

partitions=topic.partitions

consumer=topic.get_simple_consumer(consumer_group=b"g1")

#消费者从指定的offset消费
consumer.reset_offsets(partition_offsets=[(partitions[0],981208),
                                          (partitions[1],981208),
                                          (partitions[2],981208)])
for message in consumer:
    pid=message.partition_id
    data=message.value
    offset=message.offset
    print(f"partition_id:{pid}  offset:{offset}  message:{data}")
    consumer.commit_offsets()