#-*- coding: utf-8 -*-
# @Time    : 2019/3/9 14:37
# @Author  : Z
# @Email   : S
# @File    : temp_opt.py
import pandas as pd
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pysparkDemo.lng_lat import get_spark
# df=pd.read_csv("E://dataset//yancao//餐饮//part-00000.csv")
# print(df)

spark=get_spark()
df0=spark.read.csv("E://dataset//yancao//餐饮//part-00000.csv",header=True,inferSchema=True)

df0.printSchema()
df0.show()

df0.select(count(df0.lng),min(df0.haversine),max(df0.haversine)).show()

df0.where(df0.haversine<1000).show()

