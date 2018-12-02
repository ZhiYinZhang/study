#-*- coding: utf-8 -*-
# @Time    : 2018/3/25 14:50
# @Author  : Z
# @Email   : S
# @File    : HelloWorld.py
import pandas as pd
import numpy as np
import time
import os
from multiprocessing import Process


def run_proc(name):
    print(f"run child process {name}:{os.getpid()}")
    data.append('child')
    print()

if __name__=="__main__":
    data=[1,2,3,4]
    print(f"parent process {os.getpid()}")
    p=Process(target=run_proc,args=('test',))
    print("process will start")
    p.start()
    p.join()

    print(data)
    print("process end")