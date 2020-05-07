#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/12/2 14:55
from pyspark.sql import SparkSession,functions as f

spark=SparkSession.builder\
                  .appName("mysql binlog")\
                  .master("local[*]")\
                  .getOrCreate()
df = spark.readStream\
    .format("org.apache.spark.sql.mlsql.sources.MLSQLBinLogDataSource")\
    .option("host","10.18.0.34")\
    .option("port","3306")\
    .option("userName","zhangzy")\
    .option("password","123456")\
    .option("databaseNamePattern","aistrong")\
    .option("tableNamePattern","test1")\
    .option("binlogIndex", "1") \
    .option("binlogFileOffset", "49124") \
    .option("bingLogNamePrefix","binlog")\
    .load()
#bingLogNamePrefix  binlog文件前缀

# df.writeStream\
#     .format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource")\
#     .option("__path__","e://test//delta/test1/tmp/sysn/tables")\
#     .option("mode","Append")\
#     .option("idCols","id")\
#     .option("duration","5")\
#     .option("syncType","binlog")\
#     .option("checkpointLocation","e://test//delta/test1/tmp/cpl-binlog2")\
#     .outputMode("append")\
#     .start("e://test//delta//test1/{db}/{table}")\
#     .awaitTermination()

df.printSchema()
df.writeStream.format("console")\
      .outputMode("append")\
      .option("truncate","false")\
      .start().awaitTermination()