#-*- coding: utf-8 -*-
# @Time    : 2019/4/20 23:11
# @Author  : Z
# @Email   : S
# @File    : hbase_test.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import psutil
import time
print(psutil.virtual_memory())
spark = SparkSession.builder.enableHiveSupport().appName("area").getOrCreate()
df=spark.range(100000).withColumn("index",f.col("id"))
print(psutil.virtual_memory())
# df.show()
# del(df)
for col in df.columns:
    df.drop(col)
time.sleep(10)
print(psutil.virtual_memory())