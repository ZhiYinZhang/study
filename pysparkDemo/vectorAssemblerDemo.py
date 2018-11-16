#!/usr/bin/env python
# -*- coding:utf-8 -*-
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.types import *
spark=SparkSession.builder \
    .appName("vectorAssembler") \
    .master("local[3]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

dataset = spark.createDataFrame(
    [(0,18,1.0,Vectors.dense([0.0,10.0,0.5]),1.0),
(1,19,0.5,Vectors.dense([0.1,11.0,0.9]),3.0),
(2,15,0.9,Vectors.dense([0.1,12.0,0.5]),7.0),
(3,16,1.1,Vectors.dense([0.2,11.0,0.7]),9.0),
(4,11,1.3,Vectors.dense([0.4,8.0,0.7]),11.0),
(5,12,0.7,Vectors.dense([0.6,11.0,0.2]),7.0),
(6,17,0.8,Vectors.dense([0.2,17.0,0.6]),8.0),
(7,18,1.5,Vectors.dense([0.3,14.0,0.4]),3.0),
(8,10,2.0,Vectors.dense([0.1,13.0,0.9]),5.0),
(9,9,0.6,Vectors.dense([0.2,14.0,0.3]),12.0),
(10,17,0.8,Vectors.dense([0.4,8.0,0.8]),10.0),
(11,11,0.7,Vectors.dense([0.5,9.0,0.7]),11.0),
(12,15,0.9,Vectors.dense([0.7,11.0,0.6]),9.0)],
    ["id", "hour", "mobile", "userFeatures", "clicked"])


assembler=VectorAssembler(inputCols=["hour","mobile","userFeatures"],
                outputCol="features")
output:DataFrame = assembler.transform(dataset)
output.show(10)

output.printSchema()

output.write.format("json").save("E:\\test\\dpt\\vector",mode="overwrite")

spark.stop()