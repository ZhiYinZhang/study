#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/11 13:12
from pyspark.sql import SparkSession,DataFrame,Row
from pyspark.sql import functions as f
from pyspark.sql.functions import col
from pyspark import StorageLevel
import time
from datetime import datetime as dt
from application.tobacco_rules.rules import write_hbase2
import os
if __name__=="__main__":
    spark: SparkSession = SparkSession.builder \
        .appName("demo") \
        .master("local[3]") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    df=spark.range(10)

    root_dir = "E:\资料\project\烟草\外部数据\人流数据\\530"
    # 人流数据
    shaoyang_vfr_path = os.path.join(root_dir, "shaoyang")
    yueyang_vfr_path = os.path.join(root_dir, "yueyang")
    zhuzhou_vfr_path = os.path.join(root_dir, "zhuzhou")

    shaoyang_vfr = spark.read.csv(shaoyang_vfr_path, header=True)\
                            .withColumn("cityname",f.lit("邵阳市"))\
                             .withColumn("citycode", f.lit("430500"))
    yueyang_vfr = spark.read.csv(yueyang_vfr_path, header=True)\
                             .withColumn("cityname", f.lit("岳阳市"))\
                             .withColumn("citycode", f.lit("430600"))
    zhuzhou_vfr = spark.read.csv(zhuzhou_vfr_path, header=True)\
                             .withColumn("cityname", f.lit("株洲市"))\
                             .withColumn("citycode", f.lit("430200"))

    vfr = shaoyang_vfr.unionByName(yueyang_vfr).unionByName(zhuzhou_vfr)



    shaoyang_vfr.show()
    yueyang_vfr.show()
    result=vfr.withColumn("date",f.to_timestamp(col("time"),"yyyyMMdd_HHmmss"))\
                .withColumn("weekday",f.dayofweek(col("date")))\
                .withColumn("hour",f.hour(col("date")))\
                .withColumn("time_week",f.when(col("weekday")==1,"周末").when(col("hour")>12,"下午").when(col("hour")<12,"上午"))
    #
    hbase = {"table": "V530_.PEOPLE_STREAM", "families": ["0"], "row": "cust_id"}
    result.foreachPartition(lambda x:write_hbase2(x,["count","wgs_lng","wgs_lat","time","cityname","citycode","time_week"],hbase))


