#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/29 16:48
from pysparkDemo.predict_rent import *

from pyspark.sql.types import StructType


def f(iterators):

    print(type(iterators))

    return iterators

filePath = "E:\\test\\csv\\smart"

spark = SparkSession.builder \
            .appName("smartCity")\
            .master("local[3]")\
            .getOrCreate()

params =[
      ("rent_id","string"),
    ("address", "string"),
    ("rent", "string"),
    ("rent_type", "string"),
    ("area", "string"),
    ("decorate", "string"),
    ("longitude", "string"),
    ("latitude", "string"),
    ("county_id", "string")]

schema = StructType()
for param in params:
    schema.add(param[0],param[1])


df_1 = spark.readStream \
     .format("csv") \
     .option("header","true") \
     .schema(schema) \
     .load(filePath)


df_1 = df_1.repartition(1)



df_1.writeStream.foreach

