#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2018/12/13 16:48
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

def ForeachBatch(batchDF:DataFrame,batchId:int):
        batchDF.persist()

        batchDF.write.format("csv").mode("append").option("header","true").save("e://test//public//csv")
        batchDF.write.format("json").mode("append").save("e://test//public//json")
        batchDF.unpersist()

spark = SparkSession.builder \
            .appName("foreachBatch") \
            .master("local[2]") \
            .getOrCreate()

df1 = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond",10) \
    .option("numPartitions",1) \
    .load()
df2 :DataFrame= df1.selectExpr("value % 10 as key") \
    .groupBy("key") \
    .count() \
    .toDF("key","value") \


df2.writeStream \
    .foreachBatch(ForeachBatch) \
    .outputMode("update") \
    .start() \
    .awaitTermination()






# df = spark.read.csv(path="e://test/public/csv",inferSchema=True,header=True)
# df1 = df.repartition(1)
# df1.write.csv(path="e://test/public/csv1",header=True)