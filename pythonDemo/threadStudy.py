#!/usr/bin/env python
# -*- coding:utf-8 -*-
from datetime import datetime
import time
import os
import sys
import subprocess as sp
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
        print("========%s %s======"%(self.threadName,threads))
        try:
          printTime(self.threadName,self)
        except BaseException as exe:
          print (exe)
          delete(self.threadName,threads)
        # print("exit-----------%s"%self.threadName)
        print("==========%s %s=========="%(self.threadName,threads))
    def printFlag(self):
        self.flag=True

def printTime(threadName,th):
    for i in range(0,1000):
      if not th.flag:
        t=time.strftime('%Y-%m-%d %H:%M:%S')
        print('%s-----%s-----%s'%(threadName,i,t))
        time.sleep(1)
      else:
        # sys.exit()
        3/0
        print ('mei you tui chu')

def delete(topic,threads):
    del(threads[topic])

threads={}
with open('E:/test/topic.txt', mode='r') as file:
    topics = file.read().splitlines()
    for topic in topics:
        t=myThread(topic)
        threads[topic]=t
        t.start()



# with open('e:/test/topic.txt',mode='w') as file:
#
#     for i in topic:
#         file.write(i+'\n')
#
# def demo(topic):
#     topic.append('demo')
# demo(topic)
# print id(topic)


# for i in l:
#     pid=str(i).split(' ').pop()[:-2]
#     print pid
e=threading.enumerate()
print ('e type',type(e))

for i in range(10):
    print (threads)
    print (threading.enumerate())
    print ('main-------%s'%i)
    time.sleep(2)
    if i==5:
      threads['app2'].printFlag()



