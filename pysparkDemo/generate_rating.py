#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/4/29 15:44
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.sql import functions as f
from pyspark.sql.functions import col
from pyspark.sql import Window
from pyspark.sql.types import TimestampType
from datetime import datetime as dt
from datetime import timedelta
import time

"""
nohup \
spark2-submit --name generate_rating \
      --master yarn \
      --deploy-mode client \
      --num-executors 5 \
      --executor-memory 5 \
      --executor-cores 3 \
      --conf spark.executor.memoryOverhead=512m \
      --conf spark.pyspark.python=/home/hadoop/anaconda3/bin/python \
	  generate_rating.py \
>generate_rating.log 2>&1 &
"""

kafka_bootstrap_servers="10.18.0.12:9092,10.18.0.28:9092,10.18.0.32:9092"
topic="rating"
checkpoint="/user/zhangzy/rating_checkpoint"


spark=SparkSession.builder.appName("generate_rating").getOrCreate()



result1=spark.read.parquet("/user/zhangzy/dataset/ratings_join")

# 将日期往后推

#2020-01-06 17:57:00.170000
#min_dt=result1.agg(f.min("CreateTime")).collect()[0][0]

time_diff=(dt(2020,4,28)-dt(2020,1,17))

def add_day(c,days):
    return c+timedelta(days)
add_day_udf=f.udf(add_day,TimestampType())


result2=result1.withColumn("timestamp",add_day_udf("Createtime",f.lit(time_diff.days)))\
               .select("userId","movieId","rating","timestamp","CreateTime")

result2.persist(StorageLevel.DISK_ONLY)
print(result2.count())


#当天的数据
static_df=result2.where(f.to_date("timestamp")==f.current_date())

#rate源每毫秒生成一条数据
rate_df=spark.readStream\
             .format("rate")\
             .option("rowsPerSecond",1000)\
             .load()

join_static=rate_df.join(static_df,"timestamp")\
            .select(f.to_json(f.struct("userId","movieId","rating","timestamp")).alias("value"))


query=join_static.writeStream\
                 .format("kafka")\
                                 .outputMode("append")\
                                 .option("kafka.bootstrap.servers",kafka_bootstrap_servers)\
                                 .option("topic",topic)\
                                 .option("checkpointLocation",checkpoint)\
                                 .start()
query.awaitTermination()