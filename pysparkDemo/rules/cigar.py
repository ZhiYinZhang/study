#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/6/14 14:18
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as f
from pysparkDemo.rules.utils import period_udf,get_co_co_line,week_diff,element_at
from pysparkDemo.rules.config import cities

spark=SparkSession.builder.enableHiveSupport().appName("tobacco").getOrCreate()

spark.sql("use aistrong")

def get_item_stats_info_daily():
    #本月当前本市/区订货量
    #本月当前本市/区订单额

    #获取本月截止订单行表数据
    co_co_line=get_co_co_line(spark,scope=[0,0],filter="month")\
                      .select("item_id","qty_ord","amt","sale_center_id","com_id")
    #本月每款烟在每个区的订单量,订单额
    co_co_line.groupBy(["com_id","sale_center_id","item_id"])\
                  .agg(f.sum(col("qty_ord")).alias("qty_ord"),f.sum(col("amt")).alias("amt"))


def get_item_stats_info_monthly():
    # 每个零售户每款卷烟上一月销量/环比/同比
    # 每个零售户每款卷烟上一月订单数/环比/同比
    # 每个零售户每款卷烟上一月销量占比/环比/同比

    # 获取上一个月订单行表数据
    co_co_line = get_co_co_line(spark, scope=[1, 1], filter="month") \
                 .select("item_id", "cust_id", "qty_ord")


    # 1、2.每个零售户每款烟的订单量，订单数
    qty_amt = co_co_line.groupBy("cust_id", "item_id").agg(f.sum(col("qty_ord")).alias("qty_ord"),
                                                           f.count(col("cust_id")).alias("amount"))

    # 3.每个零售户每款卷烟销量占比
    qty_ratio = co_co_line.groupBy("cust_id") \
        .agg(f.sum(col("qty_ord")).alias("qty_ord_total")) \
        .join(qty_amt, "cust_id") \
        .withColumn("qty_ratio", col("qty_ord") / col("qty_ord_total"))


    # --------------------环比

    # 获取上第二个月订单行表数据
    co_co_line = get_co_co_line(spark, scope=[2, 2], filter="month") \
                               .select("item_id", "cust_id", "qty_ord")

    # 每个零售户每款卷烟订单量，订单数 上次同期
    qty_amt_last = co_co_line.groupBy("cust_id", "item_id").agg(f.sum(col("qty_ord")).alias("qty_ord_last"),
                                                                f.count(col("cust_id")).alias("amount_last"))
    # 每个零售户每款卷烟销量占比 上次同期
    qty_ratio_last = co_co_line.groupBy("cust_id") \
                                .agg(f.sum(col("qty_ord")).alias("qty_ord_total_last")) \
                                .join(qty_amt_last, "cust_id") \
                                .withColumn("qty_ratio_last", col("qty_ord_last") / col("qty_ord_total_last"))

    #4、5.每个零售户每款卷烟订单量，订单额环比
    qty_amt_last.join(qty_amt, ["cust_id", "item_id"]) \
                .withColumn("qty_ring_rate", period_udf(col("qty_ord"), col("qty_ord_last"))) \
                .withColumn("amt_ring_rate", period_udf(col("amount"), col("amount_last")))

    #6.每个零售户每款卷烟订单量占比环比
    qty_ratio_last.join(qty_ratio, ["cust_id", "item_id"]) \
        .withColumn("qty_ratio_ring", period_udf(col("qty_ratio"), col("qty_ratio_last")))



    #---------------------同比

    # 获取去年同期 上一月订单行表数据
    last_year = get_co_co_line(spark, scope=[13, 13], filter="month") \
                             .select("item_id", "cust_id", "qty_ord")

    # 每个零售户每款烟的订单量,订单数 去年同期
    qty_amt_ly = last_year.groupBy("cust_id", "item_id") \
                          .agg(f.sum(col("qty_ord")).alias("qty_ord_ly"), f.count(col("cust_id")).alias("amount_ly"))

    # 每个零售户每款烟的占比 去年同期
    qty_ratio_ly = last_year.groupBy("cust_id") \
                            .agg(f.sum(col("qty_ord")).alias("qty_ord_total")) \
                            .join(qty_amt_ly, "cust_id") \
                            .withColumn("qty_ratio_ly", col("qty_ord") / col("qty_ord_total"))

    #7、8. 每个零售户每款卷烟订单量,订单数 同比
    qty_amt_ly.join(qty_amt, ["cust_id", "item_id"]) \
                .withColumn("qty_ord_yoy", period_udf(col("qty_ord"), col("qty_ord_ly"))) \
                .withColumn("amt_yoy", period_udf(col("amount"), col("amount_ly")))
    #9.每个零售户每款烟销量占比的同比
    qty_ratio_ly.join(qty_ratio, ["cust_id", "item_id"]) \
        .withColumn("qty_ratio_yoy", period_udf(col("qty_ratio"), col("qty_ratio_ly")))


def get_item_stats_info_weekly():
        # 每个零售户每款卷烟上一周销量/环比/同比

        # 获取上一周订单行表数据
        co_co_line = get_co_co_line(spark, scope=[1, 1], filter="week") \
                           .select("item_id", "cust_id", "qty_ord")

        #1.每个零售户每款烟的订单量
        qty_amt = co_co_line.groupBy("cust_id", "item_id").agg(f.sum(col("qty_ord")).alias("qty_ord"))


        # -------------------环比
        # 获取上第二周订单行表数据
        co_co_line = get_co_co_line(spark, scope=[2, 2], filter="week") \
                          .select("item_id", "cust_id", "qty_ord")

        # 每个零售户每款卷烟销量 上次同期
        qty_amt_last = co_co_line.groupBy("cust_id", "item_id").agg(f.sum(col("qty_ord")).alias("qty_ord_last"))

        #2.每个零售户每款卷烟销量 环比
        qty_amt_last.join(qty_amt, ["cust_id", "item_id"]) \
            .withColumn("qty_ring_rate", period_udf(col("qty_ord"), col("qty_ord_last")))


        #--------------------同比
        last_year = spark.sql("select  * from DB2_DB2INST1_CO_CO_LINE") \
                            .where(col("com_id").isin(cities)) \
                            .withColumn("born_date", f.to_date("born_date", "yyyyMMdd")) \
                            .withColumn("last_year_today", f.date_sub(f.current_date(), 365)) \
                            .withColumn("week_diff", week_diff("last_year_today", "born_date")) \
                            .where(col("week_diff") == 1) \
                            .withColumn("qty_ord", col("qty_ord").cast("float")) \
                            .select("cust_id", "item_id", "qty_ord")
        # 每个零售户每款烟的订单量 去年
        qty_ord_ly = last_year.groupBy("cust_id", "item_id")\
                              .agg(f.sum(col("qty_ord")).alias("qty_ord_ly"))
        # 每个零售户每款卷烟订单量同比
        qty_ord_ly.join(qty_amt, ["cust_id", "item_id"]) \
            .withColumn("qty_ord_yoy", period_udf(col("qty_ord"), col("qty_ord_ly")))









def get_brand_stats_info_daily():
    #本月当前本市/区订货量
    #本月当前本市/区订单额

    #获取本月截止订单行表数据
    co_co_line=get_co_co_line(spark,scope=[0,0],filter="month") \
                .withColumn("brand", element_at(f.split("item_name", "\("), f.lit(1))) \
                .select("qty_ord","amt","sale_center_id","com_id")
    #本月每款烟在每个区的订单量,订单额
    co_co_line.groupBy(["com_id","sale_center_id","brand"])\
                    .agg(f.sum(col("qty_ord")).alias("qty_ord"),f.sum(col("amt")).alias("amt"))


def get_brand_stats_info_monthly():
    # 每个零售户每款卷烟上一月销量/环比/同比
    # 每个零售户每款卷烟上一月订单数/环比/同比
    # 每个零售户每款卷烟上一月销量占比/环比/同比

    # 获取上一个月订单行表数据
    co_co_line = get_co_co_line(spark, scope=[1, 1], filter="month") \
                 .withColumn("brand", element_at(f.split("item_name", "\("), f.lit(1)))\
                 .select("brand","cust_id", "qty_ord")



    # 1、2.每个零售户每款烟的订单量，订单数
    qty_amt = co_co_line.groupBy("cust_id", "brand").agg(f.sum(col("qty_ord")).alias("qty_ord"),
                                                           f.count(col("cust_id")).alias("amount"))

    # 3.每个零售户每款卷烟销量占比
    qty_ratio = co_co_line.groupBy("cust_id") \
                            .agg(f.sum(col("qty_ord")).alias("qty_ord_total")) \
                            .join(qty_amt, "cust_id") \
                            .withColumn("qty_ratio", col("qty_ord") / col("qty_ord_total"))


    # --------------------环比

    # 获取上第二个月订单行表数据
    co_co_line = get_co_co_line(spark, scope=[2, 2], filter="month") \
                          .withColumn("brand", element_at(f.split("item_name", "\("), f.lit(1))) \
                          .select("brand", "cust_id", "qty_ord")

    # 每个零售户每款卷烟订单量，订单数 上次同期
    qty_amt_last = co_co_line.groupBy("cust_id", "brand").agg(f.sum(col("qty_ord")).alias("qty_ord_last"),
                                                                f.count(col("cust_id")).alias("amount_last"))
    # 每个零售户每款卷烟销量占比 上次同期
    qty_ratio_last = co_co_line.groupBy("cust_id") \
                                .agg(f.sum(col("qty_ord")).alias("qty_ord_total_last")) \
                                .join(qty_amt_last, "cust_id") \
                                .withColumn("qty_ratio_last", col("qty_ord_last") / col("qty_ord_total_last"))

    #4、5.每个零售户每款卷烟订单量，订单数环比
    qty_amt_last.join(qty_amt, ["cust_id", "brand"]) \
                .withColumn("qty_ring_rate", period_udf(col("qty_ord"), col("qty_ord_last"))) \
                .withColumn("amt_ring_rate", period_udf(col("amount"), col("amount_last")))

    #6.每个零售户每款卷烟订单量占比环比
    qty_ratio_last.join(qty_ratio, ["cust_id", "brand"]) \
        .withColumn("qty_ratio_ring", period_udf(col("qty_ratio"), col("qty_ratio_last")))



    #---------------------同比

    # 获取去年同期 上一月订单行表数据
    last_year = get_co_co_line(spark, scope=[13, 13], filter="month") \
                                .withColumn("brand", element_at(f.split("item_name", "\("), f.lit(1))) \
                                .select("brand", "cust_id", "qty_ord")

    # 每个零售户每款烟的订单量,订单数 去年同期
    qty_amt_ly = last_year.groupBy("cust_id", "brand") \
                          .agg(f.sum(col("qty_ord")).alias("qty_ord_ly"), f.count(col("cust_id")).alias("amount_ly"))

    # 每个零售户每款烟销量的占比 去年同期
    qty_ratio_ly = last_year.groupBy("cust_id") \
                            .agg(f.sum(col("qty_ord")).alias("qty_ord_total")) \
                            .join(qty_amt_ly, "cust_id") \
                            .withColumn("qty_ratio_ly", col("qty_ord") / col("qty_ord_total"))

    #7、8. 每个零售户每款卷烟订单量,订单数 同比
    qty_amt_ly.join(qty_amt, ["cust_id", "brand"]) \
                .withColumn("qty_ord_yoy", period_udf(col("qty_ord"), col("qty_ord_ly"))) \
                .withColumn("amt_yoy", period_udf(col("amount"), col("amount_ly")))
    #9.每个零售户每款烟销量占比的同比
    qty_ratio_ly.join(qty_ratio, ["cust_id", "brand"]) \
        .withColumn("qty_ratio_yoy", period_udf(col("qty_ratio"), col("qty_ratio_ly")))


def get_brand_stats_info_weekly():
        # 每个零售户每款卷烟上一周销量/环比/同比

        # 获取上一周订单行表数据
        co_co_line = get_co_co_line(spark, scope=[1, 1], filter="week") \
                                    .withColumn("brand", element_at(f.split("item_name", "\("), f.lit(1))) \
                                    .select("brand", "cust_id", "qty_ord")

        #1.每个零售户每款烟的订单量
        qty_amt = co_co_line.groupBy("cust_id", "brand").agg(f.sum(col("qty_ord")).alias("qty_ord"))


        # -------------------环比
        # 获取上第二周订单行表数据
        co_co_line = get_co_co_line(spark, scope=[2, 2], filter="week") \
                                    .withColumn("brand", element_at(f.split("item_name", "\("), f.lit(1))) \
                                    .select("brand", "cust_id", "qty_ord")

        # 每个零售户每款卷烟销量 上次同期
        qty_amt_last = co_co_line.groupBy("cust_id", "brand").agg(f.sum(col("qty_ord")).alias("qty_ord_last"))

        #2.每个零售户每款卷烟销量 环比
        qty_amt_last.join(qty_amt, ["cust_id", "brand"]) \
            .withColumn("qty_ring_rate", period_udf(col("qty_ord"), col("qty_ord_last")))


        #--------------------同比
        last_year = spark.sql("select  * from DB2_DB2INST1_CO_CO_LINE") \
                            .where(col("com_id").isin(cities)) \
                            .withColumn("born_date", f.to_date("born_date", "yyyyMMdd")) \
                            .withColumn("last_year_today", f.date_sub(f.current_date(), 365)) \
                            .withColumn("week_diff", week_diff("last_year_today", "born_date")) \
                            .where(col("week_diff") == 1) \
                            .withColumn("qty_ord", col("qty_ord").cast("float")) \
                            .withColumn("brand", element_at(f.split("item_name", "\("), f.lit(1))) \
                            .select("brand", "cust_id", "qty_ord")
        # 每个零售户每款烟的订单量 去年
        qty_ord_ly = last_year.groupBy("cust_id", "brand")\
                              .agg(f.sum(col("qty_ord")).alias("qty_ord_ly"))
        # 每个零售户每款卷烟订单量同比
        qty_ord_ly.join(qty_amt, ["cust_id", "brand"]) \
            .withColumn("qty_ord_yoy", period_udf(col("qty_ord"), col("qty_ord_ly")))