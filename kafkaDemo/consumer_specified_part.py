#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/4/30 10:29
from pykafka import KafkaClient
from pykafka.topic import Topic
from pykafka.partition import Partition
from pykafka.protocol import Message
import time
"""
1.指定消费某个分区
2.获取消费者拥有该topic的offset，
3.指定消费offset
"""
client=KafkaClient("10.18.0.32:9092")
topic:Topic=client.topics[b"test1"]
partitions=topic.partitions

consumer=topic.get_simple_consumer(consumer_group=b"g1",
                                   auto_commit_enable=True,
                                   auto_commit_interval_ms=1000,
                                   partitions=[partitions[0]] #指定消费某个分区
                                   )

#当前消费者拥有各分区offset情况：{分区id:偏移量id}
held_offsets=consumer.held_offsets
#{0: 393968, 2: 394037, 1: 394120}，指定分区的话就只有指定分区的信息{0:393968}
print(held_offsets)

#指定该消费者组/消费者 的 offset
consumer.reset_offsets(partition_offsets=[(partitions[0],held_offsets[0]-10)])


for message in consumer:
    message:Message=message

    value = message.value
    pid = message.partition_id
    offset = message.offset

    print(f"partition_id:{pid} offset:{offset}  message:{value}")
