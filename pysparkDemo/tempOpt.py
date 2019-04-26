#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/11 13:12
from pyspark.sql import SparkSession,DataFrame,Row
from pyspark.sql import functions as f
from pyspark.sql.functions import col
from pyspark import StorageLevel
import time
from datetime import datetime as dt
if __name__=="__main__":
    spark: SparkSession = SparkSession.builder \
        .config("spark.driver.extraClassPath=E:\mysql-connector-java-5.1.6.jar") \
        .config("spark.executor.extraClassPath=E:\mysql-connector-java-5.1.6.jar") \
        .appName("demo") \
        .master("local[3]") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    df = spark.range(100000000).withColumn("value", f.when(col("id") % 3 == 0, 3).when(col("id") % 5 == 0, 5).when(
        col("id") % 7 == 0, 7).otherwise(1)).persist()

    print(str(dt.now()))

    df.count()
    mean = df.groupBy("value").agg(f.mean("id").alias("mean"))
    mean.show()
    stddev = df.groupBy("value").agg(f.stddev_pop("id").alias("stddev"))
    stddev.show()
    var = df.groupBy("value").agg(f.var_pop("id").alias("var"))
    var.show()

    print(str(dt.now()))