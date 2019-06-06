#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/6/6 11:38
from pyspark.sql import SparkSession

shc_dpd="E://test/shc_dpd/*"
spark = SparkSession.builder.appName("spark hbase") \
        .master("local[2]") \
        .config("spark.driver.extraClassPath", shc_dpd) \
        .config("spark.executor.extraClassPath", shc_dpd) \
        .getOrCreate()


catalog="""{
        "table":{"namespace":"default","name":"test1"},
        "rowkey":"key",
        "columns":{
        "row_key":{"cf":"rowkey","col":"key","type":"string"},
        "age":{"cf":"0","col":"age","type":"string"},
        "name":{"cf":"0","col":"name","type":"string"}
        }
      }"""


df=spark.read.option("org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog.tableCatalog",catalog)\
      .format("org.apache.spark.sql.execution.datasources.hbase")\
      .load()
df.show()