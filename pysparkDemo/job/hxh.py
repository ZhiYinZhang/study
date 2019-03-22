#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/3/21 11:01
from pyspark.sql import SparkSession,DataFrame,Row
from pyspark.sql.functions import lit,col,sum,current_date,to_date,datediff,date_sub,min,max
import time

def get_lately():
    """
        统计每个烟在每个零售户的订单量(近一周，近一个月，近半年)
    :return:
    """
    path="e://test/temp//order.csv"
    df:DataFrame=spark.read.csv(path,inferSchema=True,header=True)
    #item_id,cust_id,qty_ord,born_date
    df=df.withColumn("born_date",to_date(col("born_date").cast("string"),"yyyyMMdd")) \
         .withColumn("current_date",date_sub(current_date(),78)) \
         .withColumn("date_diff",datediff(col("current_date"),col("born_date")))

    df=df.filter(df.date_diff<180)

    day_7=df.where(df["date_diff"]<7).groupBy(["item_id","cust_id"]).agg(sum("qty_ord").alias("qty_ord")).withColumn("lately",lit(7))
    day_30=df.where(df["date_diff"]<30).groupBy(["item_id","cust_id"]).agg(sum("qty_ord").alias("qty_ord")).withColumn("lately",lit(30))
    day_180=df.where(df["date_diff"]<180).groupBy(["item_id","cust_id"]).agg(sum("qty_ord").alias("qty_ord")).withColumn("lately",lit(180))

    df1:DataFrame=day_7.unionByName(day_30).unionByName(day_180)

    df1=df1.orderBy(["item_id","cust_id","lately"])
    return df1

if __name__=="__main__":
    spark = SparkSession.builder \
        .appName("structStreaming") \
        .master("local[2]") \
        .getOrCreate()