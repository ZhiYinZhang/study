#!/usr/bin/env python
# -*- coding:utf-8 -*-
from pyspark.sql import DataFrame,SparkSession
from pyspark.ml.feature import Word2Vec

spark = SparkSession.builder \
    .appName("word2vec") \
    .master("local[4]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df=spark.createDataFrame([("Hi i heard about spark".split(" "),),
                       ("i wish java could user case classes".split(" "),),
                       ("logistic regression models are neat".split(" "),)],['text'])

word2vec=Word2Vec(vectorSize=3,minCount=0,inputCol="text",outputCol="result")
model=word2vec.fit(df)
result=model.transform(df)

result.show(10,False)
print(type(result))