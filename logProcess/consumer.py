#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
import time
from pykafka import KafkaClient
import re
class kafkaConsumer():
    def __init__(self):
        # self.client=KafkaClient(hosts="119.29.165.154:9092")
        self.client = KafkaClient(hosts="10.18.0.11:9092,10.18.0.28:9092,10.18.0.12:9092")
        # client=KafkaClient(hosts="dmpetl01.td.com:9092,dmpetl02.td.com:9092,dmpapp01.td.com:9092,dmpapp02.td.com:9092,hadoop08.td.com:9092,dmp.td.com:9092")
        self.topic=self.client.topics["yy"]
        self.consumer=self.topic.get_simple_consumer()
    def writeFile(self,basedir):
        if not os.path.exists(basedir):
            os.mkdir(basedir)
        for message in self.consumer:
                print (message.value)
                # filename = time.strftime('%Y%m%d%H%M', time.localtime()) + '.log'
                # filepath = basedir + '\\' + filename
                # with open(filepath,'a') as file:
                #       level=re.split(r':|,',message.value)[1]
                #       file.write(message.value+'\n')
kafkaConsumer().writeFile('e:\\test\\t')