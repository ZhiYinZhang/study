#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/5/29 17:58
from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# deltaTable = "/user/delta/test2"
deltaTable = "e:/test/delta/test2"

spark = SparkSession.builder.appName("delta").master("local[2]").getOrCreate()

#写入初始数据
df=spark.range(0,10).withColumn("c0",f.lit(1))\
    .withColumn("c1",f.lit(1.0))\
    .withColumn("dt",f.current_date())

df.write.format("delta")\
        .partitionBy("dt")\
        .mode("append")\
        .save(deltaTable)


test2_tb=DeltaTable.forPath(spark,deltaTable)
test2_tb.toDF().show()

df=spark.range(5,15).withColumn("c0",f.lit(2))\
    .withColumn("c1",f.lit(2.0))\
    .withColumn("dt",f.current_date())

#字典里面的value可以是某列，也可以是具体的值(类型要一样)
test2_tb.alias("origin")\
.merge(df.alias("update"),"origin.id=update.id")\
.whenMatchedUpdate(set={"c0":"update.c0","c1":"1000"})\
.whenNotMatchedInsert(values={"id":"update.id","c0":"99999","c1":"update.c1","dt":"update.dt"})\
.execute()


test2_tb.toDF().show()