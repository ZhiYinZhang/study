#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/11/27 14:53
import time
import sys


from pyspark.sql import SparkSession
from pyspark import RDD
from pyspark.sql.functions import *
spark = SparkSession.builder \
    .appName("rdd2df")\
    .master("local[2]")\
    .getOrCreate()

df1=spark.range(0,10000).toDF("col1")
print(df1.schema)

r:RDD = df1.rdd

# spark.createDataFrame(data=r.takeSample()).show()

a = r.takeSample(withReplacement=True,num=100000)
print(type(a),len(a))


