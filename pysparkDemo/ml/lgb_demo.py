#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/29 11:31

from mmlspark import LightGBMRegressor
from pyspark.sql import SparkSession
spark=SparkSession.builder \
      .appName("structStreaming") \
      .config("spark.jars.packages","Azure:mmlspark:0.15") \
      .master("local[2]") \
      .getOrCreate()
lgb = LightGBMRegressor(alpha=0.3,learningRate=0.3,numIterations=100,numLeaves=31)