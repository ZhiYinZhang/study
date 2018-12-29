#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2018/12/29 11:40
from pyspark.sql import SparkSession

spark = SparkSession.builder \
               .appName("arrow") \
               .master("local[2]") \
               .getOrCreate()

df = spark.createDataFrame([(1,2,3),(1,1,2),(2,1,2)],['a','b','c'])

df.show()