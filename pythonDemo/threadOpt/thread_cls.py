#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/1/17 10:40
import time
from threading import Thread
"""
这种方式与直接继承Thread相比，不依赖threading库，可以
与线程有关，也可以无关，还可以通过multiprocessing模块
在一个单独的进程中执行代码
"""
class CountdownTask:
    def __init__(self):
        self._running=True
    def terminate(self):
        self._running=False
    def run(self,n):
        while self._running and n>0:
            print("T-minus",n)
            n-=1
            time.sleep(1)

c=CountdownTask()
t=Thread(target=c.run,args=(5,))
t.start()


c.terminate()
t.join()