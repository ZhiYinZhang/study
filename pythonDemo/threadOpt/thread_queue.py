#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/1/17 11:06
import time
from threading import Thread
from queue import Queue
from datetime import datetime as dt
"""
当使用队列时，协调生产者和消费者的关闭问题可能有一些麻烦，
一个通用的方法是在队列中放置一个特殊的值，当消费者读到这个值的时候，终止执行
"""
def producer(out_q):
    while running:
        data=dt.now()

        out_q.put(data)
        time.sleep(1)
    #将终止数据发送到队列
    out_q.put(_sentinel)
def consumer(in_q):
    while True:
        data=in_q.get()
        #数据等于终止数据，结束线程
        if data==_sentinel:
            #如果有多个监听这个队列的线程，需要将这个数据再put回去，这样所有消费线程都可以关闭了
            in_q.put(data)
            print("consumer exit")
            break

q=Queue()

running=True
_sentinel=object()


t1=Thread(target=producer,args=(q,))
t2=Thread(target=consumer,args=(q,))

t1.start()
t2.start()

time.sleep(10)
running=False