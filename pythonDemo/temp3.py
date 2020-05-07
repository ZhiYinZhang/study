#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/4/30 18:44
from datetime import datetime as dt
import subprocess as sp
from apscheduler.schedulers.blocking import BlockingScheduler
def submit(script):
    """

    :param script: weekly monthly
    """
    d=dt.now().strftime("%Y%m%d_%H%M%S")
    submit = f"""
            nohup spark2-submit --master yarn --deploy-mode client \
            --driver-cores 1 \
            --driver-memory 4g \
            --executor-memory 5g \
            --executor-cores 3 \
            --num-executors 5 \
            --conf "spark.pyspark.python=/opt/anaconda3/bin/python" \
            --py-files dpd.zip  \
            ./{script}.py >./log/{d}.log 2>&1 &
            """
    sp.Popen(submit, shell=True)
def submit1(script):
    d=dt.now().strftime("%Y%m%d_%H%M%S")
    submit = f"""
            spark2-submit --master yarn --deploy-mode client \
            --driver-memory 25g \
            --driver-cores 10 \
            --executor-memory 5g \
            --executor-cores 5 \
            --num-executors 10 \
            --conf "spark.executor.memoryOverhead=1g" \
            --conf "spark.pyspark.python=/opt/anaconda3/bin/python" \
            --conf "spark.driver.maxResultSize=23g" \
            --conf "spark.kryoserializer.buffer.max=1024m" \
            --py-files dpd.zip  \
            .{script}.py >./log/{d}.log 2>&1 &
            """
    sp.Popen(submit,shell=True)
def my_job():
    today=dt.now()
    day_of_month=today.day
    day_of_week=today.weekday()
    hour=today.hour
    if hour==3:
        submit("daily1")
    if day_of_week==0 and hour==10:
        submit("daily2")
    if day_of_month==1 and hour==10:
        submit("daily3")
    if day_of_week==6 and hour==3:
        submit1("daily4")

if __name__=="__main__":
    # submit("weekly")
    # submit("monthly")
    scheduler=BlockingScheduler()
    scheduler.add_job(func=my_job,trigger="cron",hour="*/1")
    scheduler.start()