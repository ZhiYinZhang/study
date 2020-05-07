#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/1/17 11:36
import time
from threading import Thread,Event
from queue import Queue
from datetime import datetime as dt
"""
如果一个线程需要在一个“消费者”线程处理完特定的数据项时立即得到通知，
你可以把要发送的数据和一个 Event 放到一起使用，这样“生产者”就可以
通过这个Event对象来监测处理的过程了
"""
def producer(out_q):
    while running:
        data=dt.now()
        evt=Event()
        print("producer data")
        out_q.put((data,evt))

        #等待消费线程处理完数据
        evt.wait()
    out_q.put(_sentinel)
    print("producer exit")
def consumer(in_q):
    while True:
        data,evt=in_q.get()

        #数据处理
        print(data,in_q.qsize())
        time.sleep(3)

        #处理完成
        evt.set()
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



