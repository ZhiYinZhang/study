#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/6/6 16:57
from datetime import datetime as dt
import subprocess as sp
from apscheduler.schedulers.blocking import BlockingScheduler
def submit(script):
    """

    :param script: weekly monthly
    """
    submit = f"""
            spark-submit --master yarn --deploy-mode client \
            --driver-cores 3 \
            --driver-memory 10g \
            --executor-memory 15g \
            --executor-cores 5 \
            --num-executors 4 \
            --conf "spark.pyspark.python=/opt/anaconda3/bin/python" \
            --conf "spark.driver.maxResultSize=5g" \
            --conf "spark.kryoserializer.buffer.max=1024m" \
            --py-files dpd.zip  \
            ./{script}.py >./run.log
            """
    sp.Popen(submit, shell=True)

def my_job():
    today=dt.now()
    day_of_month=today.day
    day_of_week=today.weekday()
    hour=today.hour
    if day_of_week==0 and hour==3:
              submit("weekly")
    if day_of_month==1 and hour==4:
              submit("monthly")

if __name__=="__main__":
    # submit("weekly")
    scheduler=BlockingScheduler()
    #
    scheduler.add_job(func=my_job,trigger="cron",hour="*/1")
    scheduler.start()















