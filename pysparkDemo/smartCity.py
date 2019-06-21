#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/29 16:48
from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import col
from pyspark.sql import functions as f
from datetime import datetime as dt
from hbaseDemo.hbaseOpt import write_hbase1


spark = SparkSession.builder \
            .appName("smartCity")\
            .config("spark.sql.execution.arrow.enabled","true") \
            .master("local[3]")\
            .getOrCreate()

df=spark.read.csv("E:\pythonProject\jupyter/cigar_sales_need0.csv",header=True)

hbase={"host":"10.18.0.34","size":10,"table":"V630_TOBACCO.BRAND_DATA",
       "row":"brand_id",
       "families":["0"]}


df.withColumn("brand_name", f.element_at(f.split("item_name", "\("),1))\
    .select("brand_name")\
    .distinct()\
    .withColumn("brand_id",f.row_number().over(Window.orderBy("brand_name")))\
    .withColumn("brand_id",col("brand_id").cast("string"))\
     .foreachPartition(lambda x:write_hbase1(x,hbase,["brand_name"]))