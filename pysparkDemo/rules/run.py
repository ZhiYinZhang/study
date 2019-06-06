#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/5/29 18:46
from datetime import datetime as dt
import time
import subprocess as sp
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler


def my_job():
    print(str(dt.now()))


# scheduler=BlockingScheduler()

scheduler=BackgroundScheduler()

#星期一 0
scheduler.add_job(func=my_job,trigger="cron",day="*/1",hour="*/1",minute="*/1")
scheduler.start()
