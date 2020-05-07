#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/1/6 9:36
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
from datetime import datetime as dt,timedelta
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                    filename="e://test//log//aps.log",
                    filemode="a"
                    )

def interval_task(x):
    print(dt.now().strftime("%Y-%m-%d %H:%M:%S"),x)
def cron_task(x):
    print(dt.now().strftime("%Y-%m-%d %H:%M:%S"), x)
def date_task(x):
    print(dt.now().strftime("%Y-%m-%d %H:%M:%S"), x)


print(str(dt.now()))

bs=BlockingScheduler()

bs.add_job(func=interval_task,args=("循环任务",),trigger="interval",seconds=5,id="interval")
#year,month,day,week,day_of_week,hour,minute,second,start_date,end_date
bs.add_job(func=cron_task,args=("定时任务",),trigger="cron",second="*/5",minute="*",id="cron")
#在next_run_time时间点运行一次
bs.add_job(func=date_task,args=("一次性任务",),trigger="date",next_run_time=dt.now()+timedelta(seconds=15),id="date")

bs._logger=logging
bs.start()