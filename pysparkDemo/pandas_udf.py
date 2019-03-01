#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2018/12/10 17:32
from pyspark.sql.functions import pandas_udf,PandasUDFType
from pyspark.sql import SparkSession
import pyspark
import time
spark:SparkSession = SparkSession \
          .builder \
          .master("local[2]") \
          .appName("pandas_udf") \
          .config("spark.sql.execution.arrow.enabled", "true") \
          .getOrCreate()

print(time.time())
df = spark.range(10000000)
print(time.time())
