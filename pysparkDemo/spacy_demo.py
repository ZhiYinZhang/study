#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/23 17:13
from datetime import datetime as dt
import time
import  subprocess as sp
def get_cust_num():
    spark = SparkSession.builder \
        .enableHiveSupport() \
        .appName("retail") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    spark.sql("use aistrong")
while True:
   time.sleep(1*60*60)
   print(dt.now())
   if dt.now().hour==3:
       sp.Popen("nohup spark-submit --master local[3] --conf '' ",shell=True)


