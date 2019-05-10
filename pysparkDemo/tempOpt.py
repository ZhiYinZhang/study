#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/11 13:12
from pyspark.sql import SparkSession,DataFrame,Row
from pyspark.sql import functions as f
from pyspark.sql.functions import col
from pyspark import StorageLevel
import time
from datetime import datetime as dt
if __name__=="__main__":
    spark: SparkSession = SparkSession.builder \
        .appName("demo") \
        .master("local[3]") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")


    def get_lng_lat():
        # -----网上爬取的经纬度
        print(f"{str(dt.now())}   经纬度")
        try:
            co_cust = get_co_cust(spark).select("cust_id")

            # 每个城市的零售户的经纬度
            cust_lng_lat = get_cust_lng_lat(spark).withColumn("cust_id", fill_0_udf(col("cust_id")))

            lng_lat = cust_lng_lat.select("cust_id", "longitude", "latitude") \
                .join(co_cust, "cust_id")
            return lng_lat
        except Exception:
            tb.print_exc()
