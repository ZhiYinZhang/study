#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/2/27 17:33
from pyspark.sql import SparkSession,DataFrame,Row
from pyspark import RDD
from pyspark.ml.recommendation import ALS

spark=SparkSession.builder.appName("als").master("local[2]").getOrCreate()


data_path="E:\资料\spark\spark-2.4.0-bin-hadoop2.7\data\mllib\\als/"
train="sample_movielens_ratings.txt"

lines:RDD=spark.read.text(data_path+train).rdd

parts=lines.map(lambda row:row.value.split("::"))

ratingsRDD = parts.map(lambda p: Row(userId=int(p[0]), movieId=int(p[1]),
                                         rating=float(p[2]), timestamp=int(p[3])))

ratings = spark.createDataFrame(ratingsRDD)
(training, test) = ratings.randomSplit([0.8, 0.2])

# Build the recommendation model using ALS on the training data
# Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
          coldStartStrategy="drop")
model = als.fit(training)


# Generate top 10 movie recommendations for each user
userRecs = model.recommendForAllUsers(10)
# Generate top 10 user recommendations for each movie
movieRecs = model.recommendForAllItems(10)

# Generate top 10 movie recommendations for a specified set of users
users = ratings.select(als.getUserCol()).distinct().limit(3)
userSubsetRecs = model.recommendForUserSubset(users, 10)
# Generate top 10 user recommendations for a specified set of movies
movies = ratings.select(als.getItemCol()).distinct().limit(3)
movieSubSetRecs = model.recommendForItemSubset(movies, 10)


# $example off$
userRecs.show()
movieRecs.show()
userSubsetRecs.show()
movieSubSetRecs.show()

spark.stop()
