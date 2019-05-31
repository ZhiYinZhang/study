#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/5/31 9:40
from pyspark.sql import SparkSession,DataFrame,Row
from pyspark.sql import functions as f
from pyspark.sql.functions import col
import os
"""
将烟草的一些外部数据合并一下

"""

def handle_food(root_dir):
    """
    root_dir目录下的目录层级
    |-root_dir
        |-shaoyang
            |-shaoyang_ch10_shop_list.csv
            |-shaoyang_ch15_shop_list.csv
            ...
        |-yueyang
            |-yueyang_ch10_shop_list.csv
            |-yueyang_ch15_shop_list.csv
            ...
        |-zhuzhou
            |-zhuzhou_ch10_shop_list.csv
            |-zhuzhou_ch15_shop_list.csv
            ...
    ch10:美食
    :param root_dir:
    :return:
    """
    root_dir = "E:\资料\project\烟草\零售户经纬度\dianping_data+lnglat"
    cities = ["shaoyang", "yueyang", "zhuzhou"]

    dfs = []
    for city in cities:
        food_path = os.path.join(root_dir, f"{city}/{city}_ch10_shop_list.csv")
        print(f"read {food_path}")
        df = spark.read.csv(food_path, header=True)
        dfs.append(df)

    food_df = dfs[0]
    for df in dfs[1:len(dfs)]:
        food_df = food_df.unionByName(df)
    return food_df
def handle_hotel(root_dir):
    """
    目前这里只有株洲
    :param root_dir:
    :return:
    """
    root_dir = "E:\资料\project\烟草\零售户经纬度\酒店数据（佛山、广州、深圳、岳阳、株洲）\株洲"
    lng_lat = spark.read.csv(root_dir + "/addresses.csv", header=True) \
        .select("shop_id", "lng", "lat")
    hotel = spark.read.csv(root_dir + "/zhuzhou_hotel_shop_list.csv", header=True) \
        .withColumn("price", f.regexp_extract(col("price"), "\d+", 0)) \
        .withColumn("city", f.lit("zhuzhou")) \
        .select("shop_id", "price", "city")

    result=lng_lat.join(hotel, "shop_id")
    return result


if __name__=="__main__":
    spark: SparkSession = SparkSession.builder \
        .appName("demo") \
        .master("local[3]") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")


    # handle_food(root_dir).coalesce(1).write.csv(root_dir+"/result",header=True)



    #shaoyang yueyang
    hotel=spark.read.csv("E:\资料\project\烟草\外部数据\租金餐饮酒店/hotel.csv",header=True)\
                .select("shop_id","city","price","lng","lat")


    zhuzhou_hotel=handle_hotel("")



    hotel.unionByName(zhuzhou_hotel).write.csv("E:\资料\project\烟草\外部数据\租金餐饮酒店/result",header=True)
