#!/usr/bin/env python
# -*- coding:utf-8 -*-
import subprocess as sp
import argparse
def start_consumer():
    command = "nohup python3 Consumer.py >./DPT.log 2>&1 &"
    try:
      sp.check_output("ps -ef|grep Consumer.py |grep -v grep", shell=True)
      print("\nconsumer is running,Stop it first\n")
    except:
      sp.Popen(command, shell=True)
def start_livy():
    command = "../livy-0.5.0-incubating-bin/bin/livy-server start"
    sp.Popen(command, shell=True)
def start_monitor():
    command = "nohup python3 monitor.py >/dev/null 2>&1 &"
    try:
        sp.check_output("ps -ef|grep monitor.py |grep -v grep", shell=True)
        print("\nmonitor is running.Stop it first\n")
    except:
        sp.Popen(command, shell=True)


def stop_consumer():
    command = "ps -ef|grep Consumer.py |grep -v grep |awk '{print$2}'|xargs kill -9"
    try:
      sp.check_output("ps -ef|grep Consumer.py |grep -v grep", shell=True)
      sp.Popen(command, shell=True)
    except:
        print("\n no consumer to stop! \n")
def stop_livy():
    command = "../livy-0.5.0-incubating-bin/bin/livy-server stop"
    sp.Popen(command, shell=True)
def stop_monitor():
    command = "ps -ef|grep monitor.py |grep -v grep |awk '{print$2}'|xargs kill -9"
    try:
      sp.check_output("ps -ef|grep monitor.py |grep -v grep", shell=True)
      sp.Popen(command, shell=True)
    except:
        print("\n no monitor to stop!\n")


def status_consumer():
    command = "ps -ef|grep Consumer.py |grep -v grep"
    try:
        sp.check_output(command, shell=True)
        print("\nconsumer is running\n")
    except:
        print("\nconsumer is not running\n")
def status_livy():
    command = "../livy-0.5.0-incubating-bin/bin/livy-server status"
    try:
        sp.check_output(command, shell=True)
        print("\nlivy is running\n")
    except:
        print("\nlivy is not running\n")
def status_monitor():
    command = "ps -ef|grep monitor.py |grep -v grep"
    try:
        sp.check_output(command, shell=True)
        print("\nmonitor is running\n")
    except:
        print("\nmonitor is not running\n")







if __name__=="__main__":
    parser = argparse.ArgumentParser()

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--start",choices=["consumer","livy","monitor","all"])
    group.add_argument("--stop",choices= ["consumer","livy","monitor","all"])
    group.add_argument("--status", choices=["consumer", "livy", "monitor", "all"])
    args = parser.parse_args()


    if args.start:
         start = args.start
         if start == 'consumer':
            start_consumer()
         elif start == "livy":
            start_livy()
         elif start == "monitor":
            start_monitor()
         elif start == "all":
             start_livy()
             start_consumer()
             start_monitor()
    elif args.stop:
         stop = args.stop
         if stop == 'consumer':
            stop_consumer()
         elif stop == "livy":
            stop_livy()
         elif stop == "monitor":
            stop_monitor()
         elif stop == "all":
            stop_monitor()
            stop_consumer()
            stop_livy()
    elif args.status:
        status = args.status
        if status == 'consumer':
           status_consumer()
        elif status == "livy":
           status_livy()
        elif status == "monitor":
           status_monitor()
        elif status == "all":
            status_consumer()
            status_livy()
            status_monitor()