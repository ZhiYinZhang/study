#!/usr/bin/env python
# -*- coding:utf-8 -*-
from datetime import datetime
import sqlalchemy
import time
import os
import sys
import subprocess as sp
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import re
import json
import threading
import random
import pykafka
from elasticsearch import Elasticsearch
# es=Elasticsearch(hosts='http://10.18.0.19',port=9200,http_auth=('elastic','elastic'))
# kafka=pykafka.KafkaClient("119.29.165.154:9092")
# t=time.strftime('%Y-%M-%d %H:%M:%S')
#
# start=time.time()
#
# for i in range(9000000):
#     pass
# end=time.time()
# print('%s-%s=%s'%(end,start,end-start))
class myThread(threading.Thread):
    def __init__(self,threadName):
        threading.Thread.__init__(self)
        self.setName(threadName)
        self.threadName=threadName
        self.flag=False

    def run(self):
        print("start----------%s"%self.threadName)
        printTime(self.threadName,self)
        print("exit-----------%s"%self.threadName)
    def printFlag(self):
        self.flag=True

def printTime(threadName,th):
    for i in range(0,1000):
      if not th.flag:
        t=time.strftime('%Y-%m-%d %H:%M:%S')
        print('%s-----%s-----%s'%(threadName,i,t))
        time.sleep(1)





with open('E:/test/topic.txt', mode='r') as file:
    topics = file.read().splitlines()
    for topic in topics:
        myThread(topic).start()
        # pass


# with open('e:/test/topic.txt',mode='w') as file:
#
#     for i in topic:
#         file.write(i+'\n')
#
# def demo(topic):
#     topic.append('demo')
# demo(topic)
# print id(topic)

l=threading.enumerate()
t=threading.current_thread
c=threading.activeCount()
# for i in l:
#     pid=str(i).split(' ').pop()[:-2]
#     print pid

print (l)
for i in l:
    if not 'Main' in str(i):
        time.sleep(10)
        i.printFlag()



