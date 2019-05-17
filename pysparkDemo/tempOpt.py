#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/11 13:12
from pyspark.sql import SparkSession,DataFrame,Row
from pyspark.sql import functions as f
from pyspark.sql.functions import col
from pyspark import StorageLevel
import time
from datetime import datetime as dt
if __name__=="__main__":
    spark: SparkSession = SparkSession.builder \
        .appName("demo") \
        .master("local[3]") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    path="E:\资料\project\烟草\租金餐饮酒店\\food"
    hotel1=spark.read.csv(path+"\shaoyang_food_0516.csv",header=True)
    hotel2 = spark.read.csv(path + "\yueyang_food_0516.csv", header=True)

    hotel=hotel1.unionByName(hotel2)


    hotel.repartition(1).write.csv("E:\资料\project\烟草\租金餐饮酒店\\food\\test",header=True,mode="overwrite")