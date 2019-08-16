#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col
from pyspark.sql import Window

import time
from datetime import datetime as dt
spark=SparkSession.builder.appName("test").master("local[*]").getOrCreate()

f.element_at
import sys
sys.exit()
from apscheduler.schedulers.blocking import BlockingScheduler

scheduler=BlockingScheduler()


scheduler.add_job(func=lambda :print(dt.now()),trigger="cron",second="*/1")
scheduler.start()


























































































































