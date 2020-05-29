#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/5/25 17:49
from application.real_time_recommendation.config import delta_path
from pyspark.sql import SparkSession
def get_spark():
    spark = SparkSession.builder \
        .getOrCreate()
    spark.sparkContext.setLogLevel("warn")
    return spark
