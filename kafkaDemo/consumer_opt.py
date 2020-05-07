#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/3/18 10:29
from pykafka import KafkaClient
from pykafka.topic import Topic
from pykafka.protocol import Message

client=KafkaClient("10.18.0.32:9092")

topic:Topic=client.topics[b"test"]

"""
simple consumer:可以指定消费的分区，且不需要自动分配
balanced：自动分配
"""

#获取消费者，并设置消费者组
consumer=topic.get_simple_consumer(consumer_group=b"g1")

#获取消费者，设置自动提交offset，和自动提交间隔，默认60000ms
# consumer=topic.get_simple_consumer(consumer_group=b"g1",auto_commit_enable=True,auto_commit_interval_ms=1)

#使用balance消费
# consumer=topic.get_balanced_consumer(consumer_group=b"g1",
#                                      managed=False,#False:使用zk来实现rebalance，True使用新的rebalance方法(kafka>=0.9)
#                                      zookeeper_connect="entrobus12:2181,entrobus28:2181/kafka")


#1.获取一条消息
# message:Message=consumer.consume()
# print(message.value)#获取消息
# print(message.partition_id)# 消息所在分区id
# print(message.offset)# 消息在分区内的偏移量



#2.循环获取消息
# for i in range(10):
# while True:
#     message:Message=consumer.consume()
#     print(message.value)

# 3.循环获取消息，手动提交offset
# for message in consumer:
#     pid=message.partition_id
#     data=message.value.decode("utf-8")
#     print(f"partition_id:{pid} {message.partition_key}  message:{data}")
#     手动提交offset
    # consumer.commit_offsets()




