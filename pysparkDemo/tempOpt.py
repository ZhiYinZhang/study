#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/11 13:12
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("tempOpt") \
    .master("local[5]") \
    .getOrCreate()

df1 = spark.range(0,1000000).toDF("col1")

df1.rdd.map()