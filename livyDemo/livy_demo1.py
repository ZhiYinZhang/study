#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2018/12/6 15:49
import requests as rq
import textwrap,pprint
from livyDemo.DPT_demo import *
import datetime
import time
if __name__=="__main__":
    livy_host = "http://entrobus28:8998"
    headers = {"Content-Type": "application/json"}
    pycode ="""
    for i in range(10):
      print(i)
    """
    # for i in range(25,27):
    #    rq.delete(url=livy_host+f"/sessions/{i}")

    rp = submit(pycode,session_url="/sessions/28")
    statement = rp.headers.get("location")
    start = datetime.datetime.now()
    #遍历获取程序运行progress
    flag = True
    while flag:
        time.sleep(1)
        result = get_status(statement)
        if result['state']=="available":
            flag = False
        print(f"progress:{result['progress']}")
    end = datetime.datetime.now()
    pprint.pprint(result)
    print(f"所用时间:{end-start}")