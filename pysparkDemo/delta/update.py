#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# from delta.tables import *
from pysparkDemo.delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql import functions as f
spark=SparkSession.builder.getOrCreate()

deltaTable=DeltaTable.forPath(spark,"e://test/delta/test1")


# deltaTable.update("id>5",{"value":"1"})
#使用spark sql表达式
#指定条件
# deltaTable.update(expr("id>5"),{"value":expr("value+1")})
# deltaTable.update(col("id")>5,{"value":expr("value+3")})
#所有数据
# deltaTable.update(set={"value":col("value")*2})

deltaTable.alias("events")

path="e://test/delta/"
#更新的数据
updateDF=spark.range(5,15).withColumn("value",f.lit(10))

deltaTable=DeltaTable.forPath(spark,path+"test1")
#merge条件为 events.id 等于 updates.id
#如果匹配上就更新，将events的value字段的值 更新为 updates的value的值
#如果没有匹配上就插入
# deltaTable.alias("events")\
#     .merge(updateDF.alias("updates"),"events.id=updates.id")\
#     .whenMatchedUpdate(set={"value":"updates.value"})\
#     .whenNotMatchedInsert(values={"id":"updates.id","value":"updates.value"})\
#    .execute()


deltaTable.alias("events")

deltaTable.vacuum