#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/5/29 14:14
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark=SparkSession.builder.appName("delta").master("local[2]").getOrCreate()

# tablePath="/user/delta/test1"
tablePath="e://test//delta//test"

def test(start,end,timestamp,replaceWhere):
    spark.range(start,end).withColumn("timestamp",f.to_timestamp(f.lit(timestamp)))\
               .withColumn("dt",f.to_date("timestamp"))\
               .withColumn("hour",f.hour("timestamp"))\
               .write.format("delta").partitionBy("dt","hour")\
               .mode("overwrite")\
               .option("replaceWhere",replaceWhere)\
               .save(tablePath)
def display():
    spark.read.format("delta").load(tablePath).show()

if __name__=="__main__":
    # 初始化
    replaceWhere="dt='2020-05-29' and hour='10'"
    timestamp="2020-05-29 10:00:00"
    test(1,5,timestamp,replaceWhere)

    display()

    #这次会覆盖掉符合replaceWhere条件的分区
    replaceWhere = "dt='2020-05-29' and hour='10'"
    timestamp = "2020-05-29 10:00:00"
    test(5, 10, timestamp, replaceWhere)

    display()

    # 这次会新增这个不存在的分区数据
    replaceWhere = "dt='2020-05-29' and hour='11'"
    timestamp = "2020-05-29 11:00:00"
    test(5, 10, timestamp, replaceWhere)

    display()