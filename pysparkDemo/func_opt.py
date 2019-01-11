#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2018/12/29 11:40
from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import *
spark = SparkSession.builder \
               .appName("arrow") \
               .master("local[3]") \
               .getOrCreate()

df1 = spark.range(0,10).toDF("col1")
df2 = spark.range(0,10).toDF("col2")


df1 = df1.withColumn("id",monotonically_increasing_id())
df2 = df2.withColumn("id",monotonically_increasing_id())

df3 = df1.join(df2,on="id",how="inner")
df3.drop("id").show()
# df1.exceptAll(df2).show(1000)

df3.rollup("col1","col2").avg().show(100)
df3.cube("col1","col2").avg().orderBy("col1").show()