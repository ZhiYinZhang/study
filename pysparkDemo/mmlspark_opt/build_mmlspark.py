#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/5/28 15:20

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp") \
            .master("local[*]")\
            .config("spark.jars.packages", "com.microsoft.ml.spark:mmlspark_2.11:1.0.0-rc1") \
            .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven") \
            .getOrCreate()

import mmlspark
from mmlspark.lightgbm import LightGBMClassifier
from mmlspark.lightgbm import LightGBMRegressor
help(mmlspark)