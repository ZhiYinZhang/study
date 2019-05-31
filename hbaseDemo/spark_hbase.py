#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/5/29 15:51
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("spark hbase")\
                    .master("local[2]")\
                    .config("spark.jars.packages",
                            "org.apache.hbase:hbase-client:1.2.0,org.apache.hbase:hbase-server:1.2.0,org.apache.spark:spark-examples_2.11:1.6.0-typesafe-001")\
                    .getOrCreate()
sc=spark.sparkContext


host = '10.72.59.89:2181'
table = 'test1'
conf = {"hbase.zookeeper.quorum": host, "hbase.mapreduce.inputtable": table}
keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"

hbase_rdd = sc.newAPIHadoopRDD("org.apache.hadoop.hbase.mapreduce.TableInputFormat",
                               "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
                               "org.apache.hadoop.hbase.client.Result",
                               keyConverter=keyConv,
                               valueConverter=valueConv,
                               conf=conf)



count = hbase_rdd.count()
print(count)
output = hbase_rdd.collect()