#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2018/12/14 10:30
from pyspark.sql import SparkSession,DataFrame

spark = SparkSession.builder \
                    .appName("limit") \
                    .master("local[2]") \
                    .getOrCreate()



df1 = spark.readStream \
     .format("rate") \
     .option("rowsPerSecond",1) \
     .option("numPartitions",1) \
     .load()

df2 = spark.readStream \
           .format("rate") \
           .option("rowsPerSecond",1) \
           .option("numPartitions",1) \
           .load()

df3 = df1.withWatermark("timestamp","1 second")\
    .join(df2.withWatermark("timestamp","2 second"),on="value")



# df2 = df1.groupBy("timestamp").count().toDF("timestamp","num")
# df3 = df2.limit(10)

df3.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate","false") \
    .trigger(processingTime="1 seconds") \
    .start() \
    .awaitTermination()