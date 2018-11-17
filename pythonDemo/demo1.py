#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/10/18 14:10
from pyspark.sql import SparkSession,Row

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
