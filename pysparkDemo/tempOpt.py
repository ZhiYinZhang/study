#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/11 13:12
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.streaming import StreamingQuery

filePath = "e://test//csv"

spark=SparkSession.builder \
      .appName("structStreaming") \
      .master("local[2]") \
      .getOrCreate()
spark.sparkContext.setLogLevel("WARN")


cols ={"3month_hangout": "integer",
      "4g_unhealth": "integer",
      "6month_avg_comsume": "double",
      "age": "integer",
      "contacts": "double",
      "current_amount": "double",
      "current_left": "double",
      "fax_app": "double",
      "finance_app": "double",
      "fly_app": "double",
      "hangout": "integer",
      "in_debt": "integer",
      "integerernet_age": "double",
      "is_black_list": "integer",
      "is_gym": "integer",
      "is_identity": "integer",
      "is_movie": "integer",
      "is_sam": "integer",
      "is_student": "integer",
      "is_tour": "integer",
      "is_wanda": "integer",
      "last_pay_amount": "double",
      "last_pay_time": "integer",
      "pur_app": "double",
      "target": "double",
      "tours_app": "double",
      "train_app": "double",
      "user_id": "string",
      "vedio_app": "double",
      "fee_sen_0": "integer",
      "fee_sen_1": "integer",
      "fee_sen_2": "integer",
      "fee_sen_3": "integer",
      "fee_sen_4": "integer",
      "fee_sen_5": "integer",
      "is_age_true": "integer",
      "total_app": "double"}

tp =StructType()

for col in cols.items():
    tp = tp.add(col[0],col[1])


schema: StructType = spark.read \
    .option("header",True) \
    .option("inferSchema",True) \
    .csv(filePath) \
    .schema



df1 = spark.readStream \
    .format("csv") \
    .option("header",True) \
    .option("seq",",") \
    .schema(schema) \
    .load(filePath) \


df1.printSchema()
query: StreamingQuery = df1.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
query.awaitTermination()
