#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/6/4 15:25
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("spark hbase") \
        .master("local[2]") \
        .config("spark.driver.extraClassPath", "E://test/hbase_dpd/*") \
        .config("spark.executor.extraClassPath", "e://test/bhase_dpd/*") \
        .getOrCreate()
sc = spark.sparkContext

host = '10.72.59.89'
table = 'test2'
keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
conf = {"hbase.zookeeper.quorum": host, "hbase.mapred.outputtable": table,
        "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
        "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}

rawData = ['3,0,name,Rongcheng', '4,0,age,Guanhua']
# 数据格式：(rowkey, [row key, column family, column name, value])
#
df=spark.read.csv("E:\资料\project\烟草\外部数据\零售户经纬度\\20190530",header=True)
df.show(1)


cols=df.columns
#f"{x['cust_id']},0,lng,{x['lng']}"
rdd1=df.rdd.map(lambda x:[x['cust_id']]+[[x['cust_id'],'0',c,x[c]] for c in cols])
df1=spark.createDataFrame(rdd1)
df1.printSchema()
df1.show()

df1.select("_1","_2").rdd.saveAsNewAPIHadoopDataset(conf=conf,keyConverter=keyConv,valueConverter=valueConv)
# rdd1.saveAsNewAPIHadoopDataset(conf=conf,keyConverter=keyConv,valueConverter=valueConv)
#




# d=sc.parallelize(rawData)\
#     .map(lambda x: (x[0], x.split(',')))
# d.saveAsNewAPIHadoopDataset(conf=conf, keyConverter=keyConv,valueConverter=valueConv)
# df=spark.createDataFrame(d)
# df.printSchema()
# df.show()
