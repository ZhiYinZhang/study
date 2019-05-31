#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/5/29 9:45
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql import functions as f


spark=SparkSession.builder.appName("spark phoenix")\
            .master("local[2]")\
            .config("spark.jars.packages","org.apache.phoenix:phoenix-core:4.14.0-HBase-1.2,org.apache.phoenix:phoenix-spark:4.14.0-HBase-1.2")\
            .getOrCreate()



spark.sparkContext.setLogLevel("WARN")


table="test3"
zkHost="10.72.59.89:2181"
ph_df=spark.read.format("org.apache.phoenix.spark")\
           .option("table",table)\
           .option("zkUrl",zkHost)\
            .load()

ph_df.show()