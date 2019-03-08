#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/11 14:59
import sys
import psutil
import os
import time
info=psutil.virtual_memory()

process=psutil.Process(os.getpid())

l=[]
i=0
last_mem=0
while True:
    i += 10
    l.append(i*i)
    time.sleep(0.1)
    mem = process.memory_info().rss

    if mem>last_mem:
        last_mem=mem
        print(mem,"byte \n",mem/1024/1024,"M")
        print("-------------------------------")
    if mem>20*1024*1024:
        print("OutOfMemory:java heap space")
