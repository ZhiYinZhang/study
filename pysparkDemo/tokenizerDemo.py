#!/usr/bin/env python
# -*- coding:utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer,HashingTF,IDF
spark=SparkSession \
        .builder \
        .appName("readFile") \
        .master("local[3]") \
        .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

wordDF=spark.createDataFrame([
    (0.0, "Hi I heard about Spark"),
    (0.0, "I wish Java could use case classes"),
    (1.0, "Logistic regression models are neat")
], ["label", "sentence"])

tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
wordsData = tokenizer.transform(wordDF)

hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
featurizedData = hashingTF.transform(wordsData)

idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)

print(type(wordsData))
print(type(featurizedData))
print(type(rescaledData))
rescaledData.show(truncate=False)
