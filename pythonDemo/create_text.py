#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/30 18:51
import re
import threading
import time

class myThread(threading.Thread):
    def __init__(self,threadName,file):
         threading.Thread.__init__(self)
         threading.Thread.setName(self,name=threadName)

         self.threadName=threadName
         self.file=file

         self.flag=True


         self.__flag=threading.Event()
         self.__flag.set()
         self.__running=threading.Event()
         self.__running.set()
    def run(self):
        print("开启线程:" + self.threadName)
        print(id(self.flag))
        test_stop(self)
        print("退出线程:" + self.threadName)
    def stop(self):
        # self.__running.clear()
        self.flag=False

def test_stop(myThread):

    for i in range(100):
        if not myThread.flag:exit()
        # if not self.__running.isSet(): break

        time.sleep(1)
        print(i)


flag=True
if (not flag) | True:
    print(2)


