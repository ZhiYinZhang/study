#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2018/12/10 17:32
from pyspark.sql.functions import pandas_udf,PandasUDFType
from pyspark.sql import SparkSession
import pyspark

spark = SparkSession \
          .builder \
          .master("local[2]") \
          .appName("pandas_udf") \
          .getOrCreate()

df = spark.read \
          .option("inferSchema","true") \
          .option("header","true") \
          .format("csv") \
          .option("path","E:\pythonProject\dataset\model01_train.txt") \
          .load()

df.show()