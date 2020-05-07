#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/4/27 16:54

from pykafka import KafkaClient
from pykafka.topic import Topic
from datetime import datetime as dt
import time


client=KafkaClient("10.18.0.32:9092")

print(client.topics)

topic:Topic=client.topics[b"test"]



file="E:\\test\\ml-25m\\ml-25m\\links.csv"


# with topic.get_producer() as producer:
#     with open(file,mode="r") as reader:
#         for i in range(10):
#             lines=reader.readlines(1)[0][:-1]
#             print(lines)
#             producer.produce(lines.encode())
#             time.sleep(2)

eao=topic.earliest_available_offsets()
lao=topic.latest_available_offsets()
for i in range(3):
    print(eao[i])
    print(lao[i])
    print("\n")