#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/3/18 10:29
from pykafka import KafkaClient
from pykafka.topic import Topic
from pykafka.protocol import Message

client=KafkaClient("10.18.0.12:9092")

topic:Topic=client.topics[b"rating"]

partitions=topic.partitions

consumer=topic.get_simple_consumer()

#消费者从指定的offset消费
consumer.reset_offsets(partition_offsets=[(partitions[0],6157508),
                                          (partitions[1],6157508),
                                          (partitions[2],6157508)])
for message in consumer:
    m:Message=message
    pid=message.partition_id
    data=message.value
    offset=message.offset
    t=m.timestamp

    print(f"partition_id:{pid}  offset:{offset}  message:{data} timestamp:{t}")
    # consumer.commit_offsets()
