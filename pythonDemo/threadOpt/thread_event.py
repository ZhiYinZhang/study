#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/1/17 11:36
import time
from threading import Thread
from queue import Queue
from datetime import datetime as dt
"""
如果一个线程需要在一个“消费者”线程处理完特定的数据项时立即得到通知，
你可以把要发送的数据和一个 Event 放到一起使用，这样“生产者”就可以
通过这个Event对象来监测处理的过程了
"""
def producer(out_q):
    while running:
        for i in range(10):
            data=dt.now()
            out_q.put(data)
        time.sleep(1)
    out_q.put(_sentinel)
    print("producer exit")
def consumer(in_q):
    while True:
        data=in_q.get()

        #数据处理
        print(data,in_q.qsize())
        time.sleep(3)

        #通知队列，数据已被处理
        in_q.task_done()
        if data==_sentinel:
            #如果有多个监听这个队列的线程，需要将这个数据再put回去，这样所有消费线程都可以关闭了
            in_q.put(data)
            print("consumer exit")
            break

q=Queue()
running=True
_sentinel=object()

# q.task_done()

t1=Thread(target=producer,args=(q,))
t2=Thread(target=consumer,args=(q,))

t1.start()
t2.start()


running=False

#阻塞直到队列中所有数据被get和处理了
q.join()


