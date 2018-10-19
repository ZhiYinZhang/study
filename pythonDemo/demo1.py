#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/10/18 14:10
import time
import threading
def demo():
    for i in range(10):
        print(i)
        time.sleep(1)

class myThread(threading.Thread):
    def __init__(self,threadName):
        threading.Thread.__init__(self)
        self.threadName = threadName
    def run(self):
        print('start',self.threadName)
        for i in range(10):
            print(i)
            time.sleep(1)
        print('stop',self.threadName)

if __name__=="__main__":

    th = myThread('test')
    print(type(th))
    th.start()
    th.join(5)
    print('退出')