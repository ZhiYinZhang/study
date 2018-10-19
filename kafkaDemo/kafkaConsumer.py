#!/usr/bin/env python
# -*- coding:utf-8 -*-
import pykafka
import sys
hosts="10.18.0.15:9193,10.18.0.19:9193"
# hosts="Entrobus08:9092,Entrobus11:9092,Entrobus12:9092"
client=pykafka.KafkaClient(hosts=hosts)
topic=client.topics["test-log"]
consumer=topic.get_simple_consumer()
for message in consumer:
    if message is not None:
       print (message.value)