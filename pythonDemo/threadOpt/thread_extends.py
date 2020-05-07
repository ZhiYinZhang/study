#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/1/17 10:43
from threading import Thread
import time
"""
通过继承Thread类来实现的线程，尽管也可以工作，但这样使代码依赖于
threading库，代码只能在线程上下文中使用。
"""
class CountdownThread(Thread):
    def __init__(self, n):
        super().__init__()
        self.n = n
    def run(self):
        while self.n > 0:

            print('T-minus', self.n)
            self.n -= 1
            time.sleep(5)

c = CountdownThread(5)
c.start()