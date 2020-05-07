#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/1/17 10:25
import time
from threading import Thread
"""
并没有太多可以对线程做的事情。你无法结束一个线程，无法给它发送信号，
无法调整它的调度，也无法执行其他高级操作。如果需要这些特性，你需要自己添加。
比如说，如果你需要终止线程，那么这个线程必须通过编程在某个特定点轮询来退出.
查看thread_cls.py
"""
def countdown(n):
    while n>0:
        print('T-minus',n)
        n-=1
        time.sleep(2)

#daemon=True,线程会后台运行，会在主线程终止时自动销毁
#如果不使用join()方法，主线程会马上结束，子线程也马上退出
t=Thread(target=countdown,args=(10,),daemon=True)
t.start()

#将线程加入到当前线程，并等待他终止
t.join()

if t.is_alive():
    print("running")
else:
    print("completed")