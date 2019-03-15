#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/11 13:12
from pyspark.sql import SparkSession,Column
from pyspark.sql.types import StructType
from pyspark.sql.streaming import StreamingQuery
from pyspark.ml.feature import StringIndexer,StringIndexerModel
from math import radians,cos,sin,asin,sqrt,pi,pow
from pyspark.sql.functions import *
#
# spark=SparkSession.builder \
#       .appName("structStreaming") \
#       .master("local[2]") \
#       .getOrCreate()
# spark.sparkContext.setLogLevel("WARN")
# df=spark.read.csv("E:\\test\city_map\\example2018-12-05-12-02-23去重结果.csv",header=True,inferSchema=True)




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

import pandas as pd
import numpy as np
import time
if __name__=="__main__":
      path0="E:\\曾基\\株洲-烟草零售.xls"
      path1="E:\曾基\株洲-coordinate-v2\餐饮\餐饮服务_餐饮相关场所_餐饮相关.xls"

      df0=pd.read_excel(path0)
      df1=pd.read_excel(path1)


      df0["temp"] = 1
      df1["temp"]=1


      df2=pd.merge(df0,df1,on=["temp"])
      df2.drop("temp",axis=1)
      print(df2.columns)



      print("start:",time.localtime(time.time()))
      df2["hs"]=df2.apply(lambda x:haversine(x.lng_x,x.lat_x,x.lng_y,x.lat_y),axis=1)
      print(df2.loc[:10,["lng_x","lat_x","lng_y","lat_y","hs"]])

      print("end:", time.localtime(time.time()))