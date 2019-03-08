#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/3/7 9:34
"""
获取内存和cpu信息
"""
import psutil
import os
import time
#获取本机资源信息
print(psutil.cpu_count())
print(psutil.disk_partitions())
print(psutil.virtual_memory())
print("开机时间",time.localtime(psutil.boot_time()))
#获取进程 资源使用信息
process=psutil.Process(os.getpid())
print(process.memory_info())



