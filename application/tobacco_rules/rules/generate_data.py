#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/5/23 17:45
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as f
from pyspark.sql import Window
from datetime import datetime as dt
import traceback as tb
from application.tobacco_rules.rules.utils import lng_l,lng_r,lat_u,lat_d,get_cust_lng_lat,get_consume_level,haversine


spark = SparkSession.builder\
                    .config("spark.driver.maxResultSize","5g")\
                    .enableHiveSupport().appName("generate data").getOrCreate()
sc=spark.sparkContext
sc.setLogLevel("WARN")

def generate_near_cust():
    """
      生成距离零售户最近的同市100个零售户 存入hdfs
      YJFL001指标
      更新:零售户经纬度数据变化
      用时40m  driver-memory 10g executor-memory 20g num-executors 5 executor-cores 4
    :param spark:
    :return:
    """
    print(f"{str(dt.now())} 生成距离零售户最近的同市100个零售户")
    try:
        cust_lng_lat = get_cust_lng_lat(spark)\
                        .withColumnRenamed("lng", "lng0") \
                        .withColumnRenamed("lat", "lat0") \
                        .withColumnRenamed("city", "city0") \
                        .withColumnRenamed("cust_id","cust_id0")\
                        .select("cust_id0", "lng0", "lat0", "city0")
        cust_lng_lat1 = cust_lng_lat\
                            .withColumnRenamed("lng0", "lng1") \
                            .withColumnRenamed("lat0", "lat1") \
                            .withColumnRenamed("cust_id0", "cust_id1") \
                            .withColumnRenamed("city0", "city1")

        cities = ["岳阳市", "邵阳市", "株洲市"]
        path = ["yueyang", "shaoyang", "zhuzhou"]


        # 零售户cust_id1 最近100个零售户 cust_id0
        win = Window.partitionBy("cust_id1").orderBy(col("distance"))

        for i in range(len(cities)):
            city = cities[i]
            print(f"{str(dt.now())}", city)
            result = cust_lng_lat.where(col("city0") == city) \
                .crossJoin(cust_lng_lat1.where(col("city1") == city)) \
                .withColumn("distance", haversine(col("lng0"), col("lat0"), col("lng1"), col("lat1")))

            result.withColumn("rank", f.row_number().over(win)) \
                .where(col("rank") <= 100) \
                .write.csv(header=True, path="/user/entrobus/tobacco_data/cust_cross_join/" + path[i], mode="overwrite")
    except Exception:
        tb.print_exc()

generate_near_cust()


def generate_around_vfr(around):
    """
    零售户周边人流数=零售户半径500m范围内平均人流
    平均人流=近30天所有记录中距离中心点最近的100个观测记录的均值（距离不超过500m，若不足100个则有多少算多少）
    更新:人流数据变化/零售户经纬度
    用时:23m  driver-memory 10g executor-memory 20g num-executors 5 executor-cores 4
    """
    print(f"{str(dt.now())} 零售户周边人流数")
    try:
        cust_lng_lat = get_cust_lng_lat(spark) \
            .withColumn("scope", f.lit(around)) \
            .withColumn("lng_l", lng_l(col("lng"), col("lat"), col("scope"))) \
            .withColumn("lng_r", lng_r(col("lng"), col("lat"), col("scope"))) \
            .withColumn("lat_d", lat_d(col("lat"), col("scope"))) \
            .withColumn("lat_u", lat_u(col("lat"), col("scope"))) \
            .select("city", "cust_id", "lng_l", "lng_r", "lat_d", "lat_u", "lng", "lat")

        shaoyang_vfr = spark.read.csv("/user/entrobus/tobacco_data/visitor_flow/shaoyang", header=True) \
                            .groupBy("wgs_lng", "wgs_lat").agg(f.sum("count").alias("count"))
        yueyang_vfr = spark.read.csv("/user/entrobus/tobacco_data/visitor_flow/yueyang", header=True) \
                            .groupBy("wgs_lng", "wgs_lat").agg(f.sum("count").alias("count"))
        zhuzhou_vfr = spark.read.csv("/user/entrobus/tobacco_data/visitor_flow/zhuzhou", header=True) \
                            .groupBy("wgs_lng", "wgs_lat").agg(f.sum("count").alias("count"))

        vfr = shaoyang_vfr.unionByName(yueyang_vfr).unionByName(zhuzhou_vfr)

        win = Window.partitionBy(col("cust_id")).orderBy(col("length"))

        # 零售户周边人流数=零售户半径500m范围内平均人流
        avg_vfr = cust_lng_lat.join(vfr, (col("wgs_lng") >= col("lng_l")) & (col("wgs_lng") <= col("lng_r")) & (
                                                col("wgs_lat") >= col("lat_d")) & (col("wgs_lat") <= col("lat_u"))) \
                            .withColumn("length", haversine(col("lng"), col("lat"), col("wgs_lng"), col("wgs_lat"))) \
                            .withColumn("rank", f.row_number().over(win)) \
                            .where(col("rank") <= 100) \
                            .groupBy("city", "cust_id") \
                            .agg(f.avg(col("count")).alias("avg_vfr"))
        avg_vfr.write.csv(header=True,path="/user/entrobus/tobacco_data/cust_avg_vfr",mode="overwrite")

    except Exception:
        tb.print_exc()


generate_around_vfr(0.5)


def generate_all_cust_cons():
    """
      得到所有零售户消费水平:根据有消费水平的零售户cust_id0 去 生成没有消费水平的零售户cust_id1的消费水平
      更新:零售户经纬度/[租金,餐饮,酒店]变化
      用时:11m  driver-memory 10g executor-memory 20g num-executors 5 executor-cores 4
      1.获取没有消费水平的零售户
      2.获取没有消费水平的零售户的经纬度
      3.获取有消费水平的零售户的经纬度
      4.两个DataFrame进行crossJoin
      5.计算cust_id1,cust_id0的距离
      6.取距离cust_id1最近的30个cust_id0
      7.求这30个cust_id0的消费水平的均值作为cust_id1的消费水平
    :param spark:
    :return:
    """
    print(f"{str(dt.now())} 生成零售户消费水平")
    try:
        path = "/user/entrobus/tobacco_data/cust_consume_level/"
        # 零售户经纬度
        cust_lng_lat = get_cust_lng_lat(spark)\
                              .select("city", "cust_id", "lng", "lat")

        # 有消费水平的零售户
        consume_level_df = get_consume_level(spark) \
                                  .select("city","cust_id", "consume_level")

        # 1.
        not_cons_cust = cust_lng_lat.select("cust_id").exceptAll(consume_level_df.select("cust_id"))
        # 2.
        not_cons_cust = not_cons_cust.join(cust_lng_lat, "cust_id") \
                                    .withColumnRenamed("cust_id", "cust_id1") \
                                    .withColumnRenamed("lng", "lng1") \
                                    .withColumnRenamed("lat", "lat1") \
                                    .withColumnRenamed("city", "city1")

        # 3.
        cons_cust = consume_level_df.join(cust_lng_lat, ["city","cust_id"]) \
                                    .withColumnRenamed("cust_id", "cust_id0") \
                                    .withColumnRenamed("lng", "lng0") \
                                    .withColumnRenamed("lat", "lat0")
        # 4. 5. 6.
        win = Window.partitionBy("cust_id1").orderBy("distance")
        nearest_30 = not_cons_cust.crossJoin(cons_cust) \
                                .withColumn("distance", haversine(col("lng1"), col("lat1"), col("lng0"), col("lat0"))) \
                                .withColumn("rank", f.row_number().over(win)) \
                                .where(col("rank") <= 30) \
                                .select("city1", "cust_id1", "consume_level")
        # 7.
        result = nearest_30.groupBy("city1", "cust_id1") \
                            .agg(f.avg("consume_level").alias("consume_level")) \
                            .withColumnRenamed("cust_id1", "cust_id") \
                            .withColumnRenamed("city1", "city")


        result.unionByName(consume_level_df)\
                .write.csv(header=True,path=path,mode="overwrite")
    except Exception:
        tb.print_exc()


generate_all_cust_cons()