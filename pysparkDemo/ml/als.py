#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/3/5 11:42

from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql.functions import *
from pyspark.ml.evaluation import RegressionEvaluator
import pandas as pd
import json
from pyspark.ml.feature import StringIndexer
from pyspark.sql.types import *
import os
param = {
    "data_dir": "E:\\test\\csv\\als\\part-00000.csv",
    "result_dir": "/home/zhangzy/dataset/als/result",
    "userCol": "cust_id",
    "itemCol": "item_id",
    "ratingCol": "ratings",
    "top": 30
}




def string2index(df: DataFrame, inputCol, outputCol):
    print(f"string2index {inputCol} to {outputCol}")
    stringIndexer = StringIndexer(inputCol=inputCol, outputCol=outputCol)
    model = stringIndexer.fit(df)
    df = model.transform(df)
    return df


def create_df():
    print("create dataFrame")
    spark = SparkSession.builder.appName("als").master("local[3]").getOrCreate()
    # 读取csv文件
    df = spark.read.csv(param["data_dir"],header=True) \
        .withColumn(param["itemCol"], col(param["itemCol"]).cast(LongType())) \
        .withColumn(param["ratingCol"], col(param["ratingCol"]).cast(DoubleType()))
    return df


def preprocess(df: DataFrame):
    print("start preprocess")
    userCol_new = param["userCol"] + "_int"
    param["userCol_new"] = userCol_new
    df_int: DataFrame = string2index(df, param["userCol"], userCol_new)
    return df_int


def get_als_model(df):
    print("train als model")
    # train
    als = ALS(maxIter=5, regParam=0.01, userCol=param["userCol_new"], itemCol=param["itemCol"],
              ratingCol=param["ratingCol"],
              coldStartStrategy="drop")
    model = als.fit(df)
    return model


def recForAllUser(df: DataFrame, model:ALSModel):
    print("recommend for all user")
    # Generate top 10 movie recommendations for each user
    userRecs: DataFrame = model.recommendForAllUsers(param.get("top", 10))
    userRecs_pd = userRecs.join(
        df.select(param["userCol"], param["userCol_new"]).dropDuplicates([param["userCol_new"]]), param["userCol_new"]) \
        .select(param["userCol"], "recommendations") \
        .toPandas()
    #     userRecs_pd.to_json(os.path.join(param["result_dir"],"userRecs.json"),index=False,orient="split")
    return userRecs_pd


def recForAllItem(df: DataFrame, model: ALSModel):
    print("recommend for all item")
    # Generate top 10 user recommendations for each item
    itemRecs = model.recommendForAllItems(param.get("top", 10))

    # 得到 userCol_new 与 userCol的映射
    df_dup = df.dropDuplicates([param["userCol_new"]])
    arrs = df_dup.select(param["userCol_new"], param["userCol"]).toJSON().collect()
    maps = {}
    for arr in arrs:
        arr = json.loads(arr)
        maps[arr[param["userCol_new"]]] = arr[param["userCol"]]

    def map_fun(row: Row):
        # list(Row)
        list_rows = row[1]
        result = []
        for r in list_rows:
            result.append(Row(maps[r[param["userCol_new"]]], r["rating"]))
        return Row(row[0], result)

    rdd0 = itemRecs.rdd.map(map_fun)
    # 定义struct type
    schema = StructType([
        StructField(param["itemCol"], IntegerType(), True), \
        StructField("recommendations", ArrayType(StructType(
            [StructField(param["userCol"], LongType(), True), StructField(param["ratingCol"], DoubleType(), True)]),
                                                 True), True)
    ])
    spark = SparkSession.builder.appName("als").master("local[3]").getOrCreate()
    df_pd = spark.createDataFrame(rdd0, schema).toPandas()
    #     df_pd.to_json(os.path.join(param["result_dir"],"itemRecs.json"),index=False,orient="split")
    return df_pd


def evaluate(test, model):
    # Evaluate the model by computing the RMSE on the test data
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="ratings",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error = " + str(rmse))


if __name__ == "__main__":
    df = create_df()
    df_int = preprocess(df)
    (train, test) = df_int.randomSplit([0.8, 0.2])
    model: ALSModel = get_als_model(train)

    result0 = recForAllUser(df_int, model)
    print(result0)
    # result0.to_json(os.path.join(param["result_dir"],"userRecs.json"),index=False,orient="split")

    # result1 = recForAllItem(df_int, model)
    # result1.to_json(os.path.join(param["result_dir"],"itemRecs.json"),index=False,orient="split")
    # print(result1)
    # evaluate(test, model)
