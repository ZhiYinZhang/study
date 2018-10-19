#!/usr/bin/env python
# -*- coding:utf-8 -*-
import threading
from datetime import datetime

import logProcess

exitFlag=0
class myThread(threading.Thread):
    def __init__(self,name):
        threading.Thread.__init__(self)
        self.name=name
        # self.log=log
    def run(self):#将要执行的代码写到run函数里面，线程创建后会直接执行run函数
        print( 'starting   %s  %s'%(datetime.now(),self.name))
        # print_time(self.name,self.delay,5)
        weiteLog(self.name)
        print( 'exiting %s %s'%(datetime.now(),self.name))

def weiteLog(threadName):
    mylog=logProcess.mylog()
    for i in range(0,50000):
        mylog.info('%s %s'%(i,datetime.now()),'user%s'%threadName,'session%s'%i)
        mylog.warning('%s %s'%(i,datetime.now()),'user%s'%threadName,'session%s'%i)
        if i%2==0:
            try:
                3/0
            except:
                mylog.error('%s %s'%(i,datetime.now()),'user%s'%threadName,'session%s'%i)

for i in range(0,100):
    myThread(i).start()