#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/3/27 9:17
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql import functions as f

spark=SparkSession.builder.getOrCreate()

df=spark.range(5)\
     .withColumn("date",f.date_format(f.current_timestamp(),"yyyyMMdd HHmmss"))\
     .withColumn("value",f.lit("1"))

path="e://test//delta//test"


#创建delta表
# df.write.format("delta").save(path)
#创建delta分区表
df.write.format("delta").partitionBy("date").save(path)


#读取delta表
spark.read.format("delta").load(path)
#读取时指定版本，或时间戳，不指定默认最新
spark.read.format("delta").option("timestampAsOf", '2020-03-27').load(path)
spark.read.format("delta").option("versionAsOf", 1).load(path)

#可以通过delta的api查看表的版本
DeltaTable.forPath(path).history().show()


#写一个已经存在的表

#使用append追加数据
df.write.format("delta").mode("append").save(path)
#使用overwrite覆盖整个表数据
df.write.format("delta").mode("overwrite").save(path)
#使用overwrite指定replaceWhere，只替换指定分区的数据，这个只能使用在分区表
df.write\
  .format("delta")\
  .mode("overwrite")\
  .option("replaceWhere", "date >= '2020-03-26' AND date <= '2020-03-27'")\
  .save(path)

#在写的时候如果数据的schema有变化
#使用mergeSchema参数合并schema
df.write.format("delta")\
    .mode("append")\
    .option("mergeSchema","true")\
    .save(path)