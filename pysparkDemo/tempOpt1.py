#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# born_datetime:2019/2/27 17:33
from pyspark.sql import SparkSession,DataFrame,Row
from pyspark.sql import functions as f
from pyspark.sql.functions import col
if __name__=="__main__":
    spark: SparkSession = SparkSession.builder \
        .appName("demo") \
        .master("local[3]") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")


    hotel=spark.read.format("csv")\
              .option("header",True)\
              .option("path","E:\大众点评+经纬度-邵阳，岳阳，佛山csv\yueyang_hotel_shop_list.csv")\
              .load()

    addr = spark.read.format("csv") \
        .option("header", True) \
        .option("path", "E:\大众点评+经纬度-邵阳，岳阳，佛山csv\\addresses(2).csv")\
        .load()


    hotel.join(addr,"shop_id")\
         .drop("address")\
         .withColumnRenamed("hotel_address_newly_added","address")\
         .repartition(1)\
         .write.format("csv")\
         .option("header",True)\
         .mode("overwrite")\
         .option("path","E:\大众点评+经纬度-邵阳，岳阳，佛山csv\\result")\
         .save()
