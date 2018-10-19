#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/10/8 10:24
import os
import sys
import traceback
import cgitb
import subprocess as sp

home = "/home/zhangzy"
def help_info():
    print("""
    --start
    --stop
    --zip
    --reboot
    """)
def start():
    sp.check_call(f"nohup python3 {home}/DPT/Consumer.py >{home}/DPT.log 2>&1 &")
    print("start success")
def stop():
    sp.check_call("ps -ef|grep Consumer.py|grep -v grep|awk '{print $2}'|xargs kill -9")
    print("stop success")
def zip():
    sp.check_call(f"rm -f {home}/DPT/lib.zip")
    sp.check_call(f"zip -q -r {home}/DPT/lib.zip {home}/DPT/lib")
    print("zip success")
def query():
    info = sp.check_output("ps -ef|grep Consumer.py |grep -v grep")
    print(info)
if len(sys.argv)>1:
    args = sys.argv[1]
    if args =="--start":
        start()
        query()
    elif args =="--stop":
        stop()
    elif args =="--zip":
        zip()
    elif args =="--reboot":
        stop()
        zip()
        start()
    elif args =="--query":
        query()
    else:
        help_info()
else:
   help_info()




