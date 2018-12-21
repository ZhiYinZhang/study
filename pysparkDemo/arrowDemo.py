#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2018/12/20 10:43
from pyspark.sql import SparkSession,DataFrame
from datetime import datetime

# spark1 = SparkSession.builder \
#     .appName("arrow") \
#     .master("local[2]") \
#     .getOrCreate()
# df1:DataFrame = spark1.read \
#       .format("csv") \
#       .option("header","true") \
#       .option("inferSchema","true") \
#       .load(path="E:\pythonProject\dataset\\model07_train.csv")
spark2 = SparkSession.builder \
               .appName("arrow") \
               .master("local[2]") \
               .config("spark.sql.execution.arrow.enabled","true") \
               .getOrCreate()
df2:DataFrame = spark2.read \
      .format("csv") \
      .option("header","true") \
      .option("inferSchema","true") \
      .load(path="E:\pythonProject\dataset\\model07_train.csv")

for i in range(100):

    # start1 = datetime.now()
    # pd1 = df1.toPandas()
    # pd1.describe()
    #
    # stop1 = datetime.now()

    start2 = datetime.now()
    pd2 = df2.toPandas()
    pd2.describe()
    stop2 = datetime.now()


    # print("第%s次:"%str(i),stop1-start1)
    print("第%s次:"%str(i),stop2-start2)