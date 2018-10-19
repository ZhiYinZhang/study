#!/usr/bin/env python
# -*- coding:utf-8 -*-
from pykafka import KafkaClient
import time
from datetime import datetime
import threading
import random
class myThread(threading.Thread):
    def __init__(self,threadName):
        threading.Thread.__init__(self)
        self.threadName=threadName
    def run(self):
        print('starting %s......'%self.threadName)
        kafka(self.threadName)
        print('exiting %s......'%self.threadName)


def kafka(threadName):
    client=KafkaClient(hosts="119.29.165.154:9092")
    # client=KafkaClient(hosts="10.18.0.11:9092,10.18.0.28:9092,10.18.0.12:9092")
    # client=KafkaClient(hosts="dmpetl01.td.com:9092,dmpetl02.td.com:9092,dmpapp01.td.com:9092,dmpapp02.td.com:9092,dmp.td.com:9092,hadoop08.td.com:9092")
    topic=client.topics["yy"]
    producer=topic.get_producer()

    list=['"ERROR"','INFO','DEBUG']
    for i in range(0,1000000):
         print( 'producer%s %s'%(threadName,i))
         level=list.pop(random.randint(0,list.__len__()-1))
         producer.produce("level:%s,producer%s: %s %s"%(level,threadName,str(i),datetime.now().__str__()))
         # time.sleep(1)
    producer.stop()
for i in range(0,100):
    myThread(i).start()
