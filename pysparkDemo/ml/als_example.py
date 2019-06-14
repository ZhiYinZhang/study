#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/6/12 17:18
from pyspark.sql import SparkSession

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql import functions as f

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("ALSExample")\
        .getOrCreate()

    ratings = spark.read.csv("e://test/als",header=True)\
                  .withColumn("userId",col("userId").cast("int")) \
                  .withColumn("movieId", col("movieId").cast("int")) \
                  .withColumn("rating", col("rating").cast("float"))


    (training, test) = ratings.randomSplit([0.8, 0.2])

    # Build the recommendation model using ALS on the training data
    # Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
              coldStartStrategy="drop")
    model = als.fit(training)


    # model.itemFactors.show(truncate=False)
    # model.userFactors.show(truncate=False)

    # # Evaluate the model by computing the RMSE on the test data
    # predictions = model.transform(test)
    # evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
    #                                 predictionCol="prediction")
    # rmse = evaluator.evaluate(predictions)
    # print("Root-mean-square error = " + str(rmse))
    #
    # # Generate top 10 movie recommendations for each user
    # movie_num=ratings.dropDuplicates(["movieId"]).count()
    userRecs = model.recommendForAllUsers(10)
    rdd1=userRecs.withColumn("recommendations",f.explode(col("recommendations")))\
            .rdd.map(lambda x:Row(userId=x[0],movieId=x[1][0],ratings=x[1][1]))

    spark.createDataFrame(rdd1).show()
            # .withColumn("itemId",f.element_at(col("recommendations"),1))\
            # .withColumn("ratings",f.element_at(col("recommendations"),2))\
            # .show()

    # # Generate top 10 user recommendations for each movie
    # movieRecs = model.recommendForAllItems(10)
    # movieRecs.show(truncate=False)
    # # Generate top 10 movie recommendations for a specified set of users
    # users = ratings.select(als.getUserCol()).distinct().limit(3)
    # userSubsetRecs = model.recommendForUserSubset(users, 10)
    # # Generate top 10 user recommendations for a specified set of movies
    # movies = ratings.select(als.getItemCol()).distinct().limit(3)
    # movieSubSetRecs = model.recommendForItemSubset(movies, 10)
    # # $example off$
    # userRecs.show()
    # movieRecs.show()
    # userSubsetRecs.show()
    # movieSubSetRecs.show()
    #
    # spark.stop()


