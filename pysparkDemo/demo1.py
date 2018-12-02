#-*- coding: utf-8 -*-
# @Time    : 2018/10/21 16:42
# @Author  : Z
# @Email   : S
# @File    : demo1.py
import pandas as pd
from pyspark.sql import SparkSession
from hdfsDemo.hdfsClient import *

spark = SparkSession.builder\
                    .appName("demo")\
                    .master("local[5]")\
                    .getOrCreate()

df = spark.read\
     .option("inferSchema","true")\
     .option("header","true")\
     .csv("e:/javacode/dataset/model06_train.csv")
df.printSchema()
df = df.repartition(1)
df.write.csv(path="e:/javacode/dataset/test",mode="overwrite",header=True)