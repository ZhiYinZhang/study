#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/9 16:00
from pyspark.sql import SparkSession
from pyspark import SparkContext,RDD

host="10.72.32.26"
table="coordinate"
conf={"hbase.zookeeper.quorum":host,
       "hbase.mapreduce.inputtable":table}

keyC = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
valC = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"

classPath="E:\dataset\hbase_jar\*"
spark:SparkSession=SparkSession.builder.master("local[2]")\
    .config("spark.driver.extraClassPath",classPath)\
    .config("spark.executor.extraClassPath",classPath)\
    .appName("read_hbase").getOrCreate()

sc:SparkContext=spark.sparkContext
hbase_rdd:RDD = sc.newAPIHadoopRDD(
"org.apache.hadoop.hbase.mapreduce.TableInputFormat",
"org.apache.hadoop.hbase.io.ImmutableBytesWritable",
"org.apache.hadoop.hbase.client.Result",
keyConverter=keyC,
valueConverter=valC,
conf=conf)


hbase_rdd.foreach(lambda x:print(x))
