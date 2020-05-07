#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/1/6 9:58
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_ALL,JobEvent,JobExecutionEvent,JobSubmissionEvent
from datetime import datetime as dt,timedelta
import logging

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                    filename="e://test//log//aps.log",
                    filemode="a"
                    )

def my_job(x):
    print(dt.now().strftime("%Y-%m-%d %H:%M:%S"),x)
def my_job2(x):
    print(dt.now().strftime("%Y-%m-%d %H:%M:%S"), x)
    print(1 / 0)



def my_listener(event):
    if event.exception:
        print(type(event))
        print("task error")
    else:
        print(type(event))
        print("task is running")

scheduler=BlockingScheduler()

scheduler.add_job(func=my_job,args=("interval task",),trigger="interval",seconds=5,id="interval_task")
scheduler.add_job(func=my_job2,args=("once task",),trigger="date",next_run_time=dt.now()+timedelta(seconds=15),id="once_task")



scheduler.add_listener(my_listener)

scheduler.start()