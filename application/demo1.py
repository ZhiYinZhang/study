#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/11/13 9:40
import time
import threading
import json

class myThread(threading.Thread):
    def __init__(self,threadName):
        threading.Thread.__init__(self)
        self.setName(threadName)
    def run(self):
        print(f"start {self.getName()} 线程")
        executor(self.getName())
        print(f"exit {self.getName()} 线程")

def executor(threadName):
    print(obj_threads)
    for i in range(100):
        time.sleep(1)
        print(f"{threadName}------{i}")

obj_threads = {}
if __name__=="__main__":
    for i in range(3):
        time.sleep(1)
        threadName = f"test{i}"
        th = myThread(threadName)
        obj_threads[threadName] = th
        th.start()
    print(threading.enumerate())
