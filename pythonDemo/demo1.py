#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/10/18 14:10
from pyspark.sql import SparkSession,Row
import time
import threading
import os
def demo():
    for i in range(10):
        print(i)
        time.sleep(1)

def ope(r:Row):
     global accumulator
     row = Row(accumulator)
     accumulator+=1
     print(accumulator )
     return row
spark = SparkSession.builder\
                    .appName("test")\
                    .master("local[3]")\
                    .getOrCreate()
sc  = spark.sparkContext
accumulator = sc.accumulator(0)
df = spark.read\
     .option("header",'true')\
     .option("inferSchema",'true')\
     .csv("e://javacode//dataset//model06_train.csv")

df.printSchema()
r = df.rdd
r.map(lambda x:print(x))

print(type(r))
    # th = myThread('test')
    # print(type(th))
    # th.start()
    # th.join(5)
    # print('退出')
print(os.path.abspath(__file__))
