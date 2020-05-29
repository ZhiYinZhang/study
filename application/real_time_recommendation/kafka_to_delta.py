#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/5/18 10:42
from pyspark.sql import functions as f
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from application.real_time_recommendation.config import kafka_bootstrap_servers,topic,delta_checkpoint,delta_path
from application.real_time_recommendation.utils import get_spark
"""
读取kafka数据，写入delta 分区表
"""
"""
nohup \
spark2-submit --name generate_rating \
      --master yarn \
      --deploy-mode client \
      --num-executors 5 \
      --executor-memory 5g \
      --executor-cores 3 \
      --py-files config.py \
      --conf spark.executor.memoryOverhead=512m \
      --conf spark.pyspark.python=/home/hadoop/anaconda3/bin/python \
	  kafka_to_delta.py \
>log/kafka_to_delta.log 2>&1 &
"""

spark=get_spark()

# 读取kafka数据
kafka_reader = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic) \
    .load()

# 根据json格式的数据获取数据的schema
schema = f.schema_of_json(
    """{"userId":"44303","movieId":"3338","rating":"3.5","timestamp":"2020-05-06T09:40:14.603+08:00"}""")

rating_df = kafka_reader \
        .withColumn("value", col("value").cast("string")) \
        .withColumn("value", f.from_json("value", schema)) \
        .selectExpr("value.movieId",
                    "cast(value.rating as float)",
                    "value.userId",
                    "cast(value.timestamp as timestamp)",
                    "timestamp as kafka_timestamp",
                    "to_date(value.timestamp) as dt"
                    )


query=rating_df.writeStream\
            .format("delta")\
            .outputMode("append")\
            .partitionBy("dt")\
            .option("path",delta_path)\
            .option("checkpointLocation",delta_checkpoint)\
            .start()
query.awaitTermination()