#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/11 14:59
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession,Row
from pyspark.sql.types import *
def f(r:Row):
    print(r.names)

spark = SparkSession.builder \
      .appName("hive") \
      .master("local[2]") \
      .config("spark.sql.execution.arrow.enabled","true") \
      .getOrCreate()
pdf = pd.DataFrame(np.random.randn(100,3),columns=["a","b","c"])

df = spark.createDataFrame(pdf)

df.show()



print(ArrayType(LongType).elementType)
