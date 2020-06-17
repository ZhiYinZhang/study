#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/6/6 11:38
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from datetime import datetime as dt
spark=SparkSession.builder\
                  .appName("shc")\
                  .config("spark.jars","file:///e://test/shc_dpd/shc-core-1.1.1-2.1-s_2.11.jar")\
                  .master("local[2]")\
                  .getOrCreate()
#引入json4s，还是包jar包冲突;难道这样没有覆盖，
#scala中，在pom文件里面导入json4s可以解决这个问题，但是在python里面有不能这样。
# .config("spark.jars.packages","org.json4s:json4s-jackson_2.11:3.2.11")\


catalog=''.join("""{
        "table":{"namespace":"default","name":"test"},
        "rowkey":"key",
        "columns":{
          "id":{"cf":"rowkey","col":"key","type":"string"},
          "age":{"cf":"0","col":"age","type":"string"},
          "name":{"cf":"0","col":"name","type":"string"}
        }
      }""".split())

#catalog = ''.join("""{
#   "table":{"namespace":"default", "name":"test"},
#   "rowkey":"key",
#   "columns":{
#   "rowkey":{"cf":"rowkey", "col":"key", "type":"string"},
#   "code":{"cf":"result", "col":"code", "type":"string"},
#   "date":{"cf":"result", "col":"date", "type":"string"},
#   "time":{"cf":"result", "col":"time", "type":"string"},
#   "price":{"cf":"result", "col":"price", "type":"float"},
#   "ratio":{"cf":"result", "col":"ratio", "type":"float"},
#   "bigratio":{"cf":"result", "col":"bigratio", "type":"float"},
#   "timestamp":{"cf":"result", "col":"timestamp", "type":"string"}
#   }
#   }""".split())


df=spark.read.options(catalog=catalog)\
    .option("zookeeper.znode.parent", "/hbase") \
    .option("hbase.zookeeper.quorum", "worker1,worker2,worker3") \
    .option("hbase.zookeeper.property.clientPort", "2181") \
    .format("org.apache.spark.sql.execution.datasources.hbase")\
    .load()

df.show()



# df=spark.range(10)\
#     .withColumn("age",f.lit("1"))\
#     .withColumn("name",f.lit("happybase"))
#
# df.show()
#
# from hbaseDemo.hbaseOpt import write_hbase1
# hbase={"host":"10.72.59.89","size":10,"table":"test","row":"id","families":["0"]}
#
# print(dt.now())
# df.foreachPartition(lambda x:write_hbase1(rows=x,hbase=hbase,cols=["age","name"]))
# print(dt.now())