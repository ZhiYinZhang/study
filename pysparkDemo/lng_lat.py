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
def get_spark():
      spark = SparkSession.builder \
            .appName("structStreaming") \
            .master("local[2]") \
            .getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
      return spark

# def get_distance(retail_store,service):

if __name__=="__main__":
      spark:SparkSession=get_spark()

      retail_store="e://dataset//yancao//株洲-烟草零售.csv"
      service="E:\dataset\yancao\株洲-coordinate-v2\餐饮\餐饮服务_餐饮相关场所_餐饮相关.csv"

      store_df=spark.read.csv(retail_store,header=True,inferSchema=True)
      service_df=spark.read.csv(service,header=True,inferSchema=True)


      for c in store_df.columns:
            store_df=store_df.withColumnRenamed(c,c+"_1")


      df_cr:DataFrame=store_df.crossJoin(service_df)

      df_ha=df_cr.select(col("*"),haversine(df_cr.lng,df_cr.lat,df_cr.lng_1,df_cr.lat_1).name("haversine"))
      df_ha.coalesce(1).write.csv(path="e://dataset//yancao//餐饮",header=True)
