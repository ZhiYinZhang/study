#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/1/17 10:14
import time
import threading
from threading import Thread,Lock
from queue import Queue
from datetime import datetime as dt
class SharedCounter:
    def __init__(self,initial_value=0):
        self._value=initial_value
        self._value_lock=Lock()
    def incr(self,delta=1):
       while True:
            #with代码块，会自动释放锁
           with self._value_lock:
               print(f"before incr:{self._value}")
               self._value+=delta
               print(f"after incr:{self._value}")
           time.sleep(1)
    def decr(self,delta=1):
       while True:
           with self._value_lock:
               print(f"before decr:{self._value}")
               self._value-=delta
               print(f"after decr:{self._value}")
           time.sleep(1)
sc=SharedCounter()
t1=Thread(target=sc.incr)
t2=Thread(target=sc.decr)
t1.start()
t2.start()