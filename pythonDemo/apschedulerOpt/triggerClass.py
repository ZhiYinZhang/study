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

def my_job(x):
    print(dt.now().strftime("%Y-%m-%d %H:%M:%S"),x)


bs=BlockingScheduler()

bs.add_job(func=my_job,args=("间隔5s",),trigger="interval",seconds=5,id="interval 5s")
bs.add_job(func=my_job,args=("间隔10s",),trigger="interval",seconds=10,id="interval 10s")

bs._logger=logging
bs.start()