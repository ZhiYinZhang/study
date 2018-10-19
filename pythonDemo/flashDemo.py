#!/usr/bin/env python
# -*- coding:utf-8 -*-
from flask import Flask, render_template
from jinja2 import Template
import sys
import time
import threading
class mythread(threading.Thread):
    def __init__(self,threadName):
        threading.Thread.__init__(self)
        self.threadName=threadName
        self.flag=False
        self.setName(threadName)

    def run(self):
        printTime(self.threadName)
    def setFlag(self):
        self.flag=True
    def stop(self):
        print('is program exit?')
        if self.flag:
             print('exit!!!')
             sys.exit()
        else:
            print('not exit!!!')

def printTime(threadName):
    for i in range(0,1000):
         time.sleep(1)
         print ('%s---%s---%s'%(threadName,i,time.strftime('%Y-%m-%d %H:%M:%S')))


test=mythread('test1')
print (type(test))
test.start()
for i in range(20):
    print ('main---',i)
    time.sleep(1)
    if i==5:
        print ('set flag')
        test.setFlag()

