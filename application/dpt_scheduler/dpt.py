#!/usr/bin/env python
# -*- coding:utf-8 -*-
import subprocess as sp
import argparse
import time
def start_consumer():
    command = "nohup python3 Consumer.py >./DPT.log 2>&1 &"
    try:
      status_consumer()
      print("\nconsumer is running,Stop it first\n")
    except:
      sp.Popen(command, shell=True)
      time.sleep(1)
      print("\n start consumer \n")
def start_livy():
    command = "../livy-0.5.0-incubating-bin/bin/livy-server start"
    try:
        sp.check_output(command, shell=True)
        print("\n start livy \n")
    except:
        print("\nlivy is running,Stop it first\n")
def start_monitor():
    command = "nohup python3 monitor.py >/dev/null 2>&1 &"
    try:
        status_monitor()
        print("\nmonitor is running,Stop it first\n")
    except:
        sp.Popen(command, shell=True)
        time.sleep(1)
        print("\n start monitor \n")


def stop_consumer():
    command = "ps -ef|grep Consumer.py |grep -v grep |awk '{print$2}'|xargs kill -9"
    try:
        status_consumer()
        sp.Popen(command, shell=True)
        time.sleep(1)
        print("\n stoping consumer \n")
    except:
        print("\n no consumer to stop! \n")
def stop_livy():
    command = "../livy-0.5.0-incubating-bin/bin/livy-server stop"
    sp.Popen(command, shell=True)
def stop_monitor():
    command = "ps -ef|grep monitor.py |grep -v grep |awk '{print$2}'|xargs kill -9"
    try:
        status_monitor()
        sp.Popen(command, shell=True)
        time.sleep(1)
        print("\n stoping monitor \n")
    except:
        print("\n no monitor to stop!\n")


def status_consumer():
    command = "ps -ef|grep Consumer.py |grep -v grep"
    sp.check_output(command, shell=True)
def status_livy():
    command = "../livy-0.5.0-incubating-bin/bin/livy-server status"
    sp.check_call(command, shell=True)
def status_monitor():
    command = "ps -ef|grep monitor.py |grep -v grep"
    sp.check_output(command, shell=True)








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
            try:
                status_consumer()
                print("\nconsumer is running\n")
            except:
                print("\nconsumer is not running\n")
        elif status == "livy":
           status_livy()
        elif status == "monitor":
           try:
              status_monitor()
              print("\nmonitor is running\n")
           except:
              print("\nmonitor is not running\n")
        elif status == "all":
            try:
                status_consumer()
                print("\nconsumer is running\n")
            except:
                print("\nconsumer is not running\n")

            status_livy()

            try:
                status_monitor()
                print("\nmonitor is running\n")
            except:
                print("\nmonitor is not running\n")