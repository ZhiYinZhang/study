#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/5/18 11:42
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col
from pyspark.sql import Window

spark=SparkSession.builder\
                  .appName("combine data")\
                  .getOrCreate()
spark.sparkContext.setLogLevel("warn")
#凤凰云数据  行数:24167207
action=spark.read.parquet("/user/zhangzy/real_time_recommendation/LogAction")

#电影评分数据 行数:25000095
ratings=spark.read.csv("/user/zhangzy/real_time_recommendation/ml-25m/ratings.csv",header=True)


#-----合并日期
#按照日期排序，新增序号列
win1=Window.orderBy("CreateTime")
action_rank=action.withColumn("rankId",f.row_number().over(win1)).repartition(200)

#按照日期排序，新增序号列
win2=Window.orderBy("timestamp")
ratings_rank=ratings.withColumn("rankId",f.row_number().over(win2)).repartition(200)


#行数要一样
result=ratings_rank.where(col("rankId")<=24167207)\
            .join(action_rank.select("CreateTime","rankId"),"rankId")


result.write\
      .format("delta")\
      .mode("overwrite")\
      .save("/user/delta/combine_data")