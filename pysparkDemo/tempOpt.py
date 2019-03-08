#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/11 13:12
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.streaming import StreamingQuery
from pyspark.ml.feature import StringIndexer,StringIndexerModel
from  math import radians,cos,sin,asin,sqrt,pi,pow
#
# spark=SparkSession.builder \
#       .appName("structStreaming") \
#       .master("local[2]") \
#       .getOrCreate()
# spark.sparkContext.setLogLevel("WARN")
# df=spark.read.csv("E:\\test\city_map\\example2018-12-05-12-02-23去重结果.csv",header=True,inferSchema=True)
#
# df.show()





#根据两个点的经纬度，计算这两个点的球面距离
def haversine(lng1,lat1,lng2,lat2):
      radius = 6378.137
      radLng1,radLat1,radLng2,radLat2=map(radians,[lng1,lat1,lng2,lat2])
      result=2.0 * asin(
            sqrt(
                  pow(sin((radLat1 - radLat2) / 2.0), 2) +
                  cos(radLat1) * cos(radLat2) * pow(sin((radLng1 - radLng2) / 2.0), 2))
      ) * radius
      return result







