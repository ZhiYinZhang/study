#!/usr/bin/env python
# -*- coding:utf-8 -*-
from pyspark.sql import DataFrame,SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.linalg import Vectors
spark = SparkSession.builder \
    .appName("word2vec") \
    .master("local[4]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

training =spark.createDataFrame((
    (1.0, Vectors.dense(0.0, 1.1, 0.1)),
    (0.0, Vectors.dense(2.0, 1.0, -1.0)),
    (0.0, Vectors.dense(2.0, 1.3, 1.0)),
    (1.0, Vectors.dense(0.0, 1.2, -0.5))
)).toDF("label", "features")

lr=LogisticRegression(maxIter=10,regParam=0.01)

model1=lr.fit(training)
print(type(model1))
result=model1.transform(training)

result.show(10)