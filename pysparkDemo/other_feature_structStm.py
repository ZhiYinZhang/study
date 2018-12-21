#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2018/12/14 10:30
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import *
from pyspark.sql.functions import sum

spark = SparkSession.builder \
      .appName("demo") \
      .master("local[3]") \
      .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

train_df = spark.read \
      .format("csv") \
      .option("header","true") \
      .option("inferSchema","true") \
      .option("path","E:\\test\\dawnbench\\train_results.csv") \
      .load()
valid_df = spark.read \
      .format("csv") \
      .option("header","true") \
      .option("inferSchema","true") \
      .option("path","E:\\test\\dawnbench\\valid_results.csv") \
      .load()
train_df.printSchema()

train_df.groupBy("epoch").agg( \
    #保留两位小数
    round( \
           # //1.将batch_duration 由timestamp -》 second  2.求和  3.转成小时
           sum(second(train_df["batch_duration"]))/60/60
        ,2)
    ).show()


