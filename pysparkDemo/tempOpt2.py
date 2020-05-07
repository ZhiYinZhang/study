#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col
from datetime import datetime as dt
import time
import jpype

spark=SparkSession.builder.appName("test")\
                  .config("spark.driver.memory","1g")\
                  .master("local[3]").getOrCreate()
spark.sparkContext.setLogLevel("warn")


df=spark.readStream\
        .format("rate")\
        .option("rowsPerSecond",1)\
        .load()

# df1=df.where(col("value")%2==0)\
#       .withColumn("end",f.current_timestamp())

df1=df.groupBy(f.window("timestamp","14 days","5 days"))\
     .agg(f.sum("value").alias("sum"))


query=df1.writeStream\
    .outputMode("update")\
    .format("console")\
    .option("truncate","false")\
    .start()

old_batchId=-1
while True:
    progress=query.lastProgress
    if progress is not None:
        batchId=progress["batchId"]
        if old_batchId!=batchId:
           print(progress["durationMs"])
           old_batchId=batchId

# df.writeStream\
#   .outputMode("append")\
#   .format("csv")\
#   .option("checkpointLocation","e://test//csv//checkpoint")\
#   .option("path","e://test//csv//2")\
#   .trigger(once=True)\
#   .start(header=True)\
#   .awaitTermination()
#






































































