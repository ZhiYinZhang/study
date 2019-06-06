#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/6/4 17:38
from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession.builder.appName("spark hbase") \
    .master("local[*]") \
    .config("spark.driver.extraClassPath", "E://test/phoenix_dpd/*") \
    .config("spark.executor.extraClassPath", "e://test/phoenix_dpd/*") \
    .getOrCreate()
sc:SparkContext = spark.sparkContext


ph_df=spark.read.format("org.apache.phoenix.spark")\
        .option("table","TOBACCO.AREA")\
        .option("zkUrl","10.72.59.89:2181")\
        .load()


# ph_df.show()
# ph_df.write.csv("e://test//retail",header=True,mode="overwrite")
# ph_df.show()
# ph_df.groupBy("SALE_CENTER_ID").count().show()

