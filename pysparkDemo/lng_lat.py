#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/3/7 16:44
from pyspark.sql import SparkSession,DataFrame,Column
from pyspark.sql.types import StructType
from pyspark.sql.functions import *

#根据两个点的经纬度，计算这两个点的球面距离
def haversine(lng1:Column,lat1:Column,lng2:Column,lat2:Column):
      radius = 6378137

      radLng1 = radians(lng1)
      radLat1 = radians(lat1)
      radLng2 = radians(lng2)
      radLat2 = radians(lat2)

      result=asin(
            sqrt(
                  pow(sin((radLat1 - radLat2) / 2.0), 2) +
                  cos(radLat1) * cos(radLat2) * pow(sin((radLng1 - radLng2) / 2.0), 2))
      )*2.0 * radius
      return result

spark=SparkSession.builder \
      .appName("structStreaming") \
      .master("local[2]") \
      .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
df=spark.read.csv("E:\\test\city_map\\example2018-12-05-12-02-23去重结果.csv",header=True,inferSchema=True)



df_id=df.withColumn("cust_id",monotonically_increasing_id())

data0,data1=df_id.randomSplit([0.5,0.5])

data1=data1.withColumnRenamed("wgs_lng","wgs_lng1").withColumnRenamed("wgs_lat","wgs_lat1")
df_cr:DataFrame=data0.crossJoin(data1)

df_ha=df_cr.select(col("*"),haversine(df_cr.wgs_lng,df_cr.wgs_lat,df_cr.wgs_lng1,df_cr.wgs_lat1).name("haversine"))


