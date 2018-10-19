#!/usr/bin/env python
# -*- coding:utf-8 -*-
from pyspark.sql import SparkSession,DataFrame,Row,Window
from pyspark.sql import functions
from pyspark.sql.types import *
import pandas as pd
import numpy as np

spark=SparkSession.builder \
    .appName("vectorAssembler") \
    .master("local[2]") \
    .getOrCreate()

df=spark.read \
    .format("json") \
    .option("inferSchema","true") \
    .load("e://test//t//json")
def addIndex(index_name, row):
    global i
    d = dict({index_name: i}, **row.asDict())
    i = i + 1
    return Row(**d)

i=0
index_name="id"
r=df.rdd.map(lambda row:addIndex(index_name,row))
df3=spark.createDataFrame(r)
df3.show()


w=Window.orderBy("c")
df4=df.withColumn("index",functions.row_number().over(w))
df4.show()