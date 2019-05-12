#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/5/8 14:37
import traceback as tb
from pyspark.sql import functions as f
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pysparkDemo.rules_temp.write_hbase import write_hbase1
from pysparkDemo.rules_temp.utils import *
from pyspark.sql import Window
from datetime import datetime as dt


spark = SparkSession.builder.enableHiveSupport().appName("retail warning").getOrCreate()
sc=spark.sparkContext
sc.setLogLevel("WARN")

spark.sql("use aistrong")

hbase={"table":"TOBACCO.RETAIL_WARNING","families":["0"],"row":"cust_id"}
hbase={"table":"test_ma","families":["info"],"row":"cust_id"}




def get_around_order_except():
    try:
        co_cust=get_valid_co_cust(spark).select("cust_id")

        # -----------------------获取co_co_01
        co_co_01=get_co_co_01(spark)\
            .withColumn("month_diff",month_diff_udf(f.year(col("born_date")),f.month(col("born_date")),f.year(col("today")),f.month(col("today"))))\
            .where(col("month_diff") == 1)\
            .select("qty_sum","amt_sum","cust_id")

        #每个零售户的订货总量 总订货额
        qty_amt_sum = co_co_01.groupBy("cust_id")\
            .agg(f.sum("qty_sum").alias("order_sum"), f.sum("amt_sum").alias("amt_sum"))


        #零售户经纬度
        cust_lng_lat=get_cust_lng_lat(spark).withColumn("cust_id0", fill_0_udf(col("cust_id"))) \
                    .select("cust_id0", "longitude", "latitude")
        #每个零售户  一公里的经度范围和纬度范围
        cust_lng_lat0 = cust_lng_lat.withColumn("scope",f.lit(1))\
            .withColumn("lng_l", lng_l(col("longitude"), col("latitude"),col("scope"))) \
            .withColumn("lng_r", lng_r(col("longitude"), col("latitude"),col("scope"))) \
            .withColumn("lat_d", lat_d(col("latitude"),col("scope"))) \
            .withColumn("lat_u", lat_u(col("latitude"),col("scope"))) \
            .withColumnRenamed("cust_id0", "cust_id1") \
            .select("cust_id1", "lng_l", "lng_r", "lat_d", "lat_u")


        # 每个零售户cust_id1 周边有cust_id0这些零售户
        around_cust=cust_lng_lat.join(cust_lng_lat0,(col("longitude")>=col("lng_l")) & (col("longitude")<=col("lng_r")) & (col("latitude")>=col("lat_d")) & (col("latitude")<=col("lat_u")))\
           .select("cust_id1","cust_id0")


        #-----零售户上个月订货条均价与周边1km范围内零售户订货条均价不符
        try:
            print(f"{str(dt.now())} 零售户上个月订货条均价与周边1km范围内零售户订货条均价不符")
            # 条均价
            avg_price = qty_amt_sum.withColumn("avg_price", col("amt_sum") / col("order_sum"))\
                                   .select("cust_id","avg_price")
            #每个零售户cust_id1 周边
            mean_std = around_cust.join(avg_price, col("cust_id0") == col("cust_id")) \
                .select("cust_id1", "avg_price") \
                .groupBy("cust_id1") \
                .agg(f.mean("avg_price").alias("mean"), f.stddev_pop(col("avg_price")).alias("stddev"))

            # 11 10 21 20 31 30  第一位表示:预警级别 1:c 2:b 3:a 第二位表示 过高:1 过低:0
            # 均值+3标准差 < 条均价 < 均值+4标准差   过高
            # 均值-4标准差 < 条均价 < 均值-3标准差   过低
            mean_std.join(avg_price, col("cust_id1") == col("cust_id")) \
                .withColumn("mean_plus_3stddev", col("mean") + col("stddev") * 3) \
                .withColumn("mean_minus_3stddev", col("mean") - col("stddev") * 3) \
                .withColumn("mean_plus_4stddev", col("mean") + col("stddev") * 4) \
                .withColumn("mean_minus_4stddev", col("mean") - col("stddev") * 4) \
                .withColumn("mean_plus_5stddev", col("mean") + col("stddev") * 5) \
                .withColumn("mean_minus_5stddev", col("mean") - col("stddev") * 5) \
                .withColumn("half_km_avg_abno",
                            f.when((col("avg_price") > col("mean_plus_3stddev")) & (
                            col("avg_price") < col("mean_plus_4stddev")), "11") \
                            .when((col("avg_price") > col("mean_minus_4stddev")) & (
                            col("avg_price") < col("mean_minus_3stddev")), "10") \
                            .when((col("avg_price") > col("mean_plus_4stddev")) & (
                            col("avg_price") < col("mean_plus_5stddev")), "21") \
                            .when((col("avg_price") > col("mean_minus_5stddev")) & (
                            col("avg_price") < col("mean_minus_4stddev")), "20") \
                            .when((col("avg_price") > col("mean_plus_5stddev")), "31") \
                            .when((col("avg_price") < col("mean_minus_5stddev")), "30")
                            )\
                .dropna(subset=["half_km_avg_abno"])\
                .show(1000)
        except Exception:
            tb.print_exc()

    except Exception:
        tb.print_exc()