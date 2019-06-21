#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/6/19 10:21
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as f
from datetime import datetime as dt
import traceback as tb
from application.tobacco_rules.rules.utils import get_co_co_line, week_diff, element_at, get_plm_item, get_area, get_phoenix_table, get_co_cust
from application.tobacco_rules.rules.config import cities
from application.tobacco_rules.rules.write_hbase import write_hbase1

spark = SparkSession.builder.enableHiveSupport().appName("tobacco").getOrCreate()

spark.sql("use aistrong")

hbase = {"table": "TOBACCO.RETAIL", "families": ["0"], "row": "row"}


# 品牌统计指标

def get_brand_stats_info_daily():
    # 本月当前本市/区各品牌订货量
    # 本月当前本市/区各品牌订单额
    # 烟品牌id，烟品牌名称
    brand = get_phoenix_table(spark, "brand")

    area = get_area(spark)
    # com_id与city的映射关系
    city = area.dropDuplicate(["com_id", "sale_center_id"]).select("com_id", "city")
    # sale_center_id与区(list)的映射关系
    county = area.groupBy("sale_center_id") \
        .agg(f.collect_list("county").alias("county")) \
        .select("sale_center_id", "county")
    markers = ["0", "2"]
    # 按照 市或区统计
    groups = ["com_id", "sale_center_id"]
    joins = [city, county]
    cols_comm = [["city", "brand_id", "brand_name", "ciga_data_marker"],
                 ["county", "sale_center_id", "brand_id", "brand_name", "ciga_data_marker"]]
    cols = [["brand_city_orders", "brand_city_order_amount"], ["brand_county_orders", "brand_county_order_amount"]]
    for i in range(len(groups)):
        group = groups[i]
        join = joins[i]
        c = cols[i]
        marker = markers[i]
        try:
            # 获取本月截止订单行表数据
            co_co_line = get_co_co_line(spark, scope=[0, 0], filter="month") \
                .withColumn("brand_name", element_at(f.split("item_name", "\("), f.lit(1))) \
                .select("qty_ord", "amt", "", group)
            # 本月每款烟在每个区的订单量,订单额
            qty_amt = co_co_line.groupBy([group, "brand_name"]) \
                .agg(f.sum(col("qty_ord")).alias(c[0]), f.sum(col("amt")).alias(c[1]))

            column = cols_comm[i] + c
            qty_amt.join(brand, "brand_name") \
                .withColumn("row", f.concat_ws("_", col(group), col("brand_id"))) \
                .join(join, group) \
                .withColumn("ciga_data_marker", f.lit(marker)) \
                .foreachPartition(lambda x: write_hbase1(x, column, hbase))
        except Exception:
            tb.print_exc()


def get_brand_stats_info_monthly():
    # 市/区每款卷烟上一月销量/环比/同比
    # 市/区每款卷烟上一月订单数/环比/同比
    # 市/区每款卷烟上一月销量占比/环比/同比

    # 烟品牌id，烟品牌名称
    brand = get_phoenix_table(spark, "brand")

    area = get_area(spark)
    # com_id与city的映射关系
    city = area.dropDuplicate(["com_id", "sale_center_id"]).select("com_id", "city")
    # sale_center_id与区(list)的映射关系
    county = area.groupBy("sale_center_id") \
        .agg(f.collect_list("county").alias("county")) \
        .select("sale_center_id", "county")

    markers = ["0", "2"]
    # 按照 市或区统计
    groups = ["com_id", "sale_center_id"]
    joins = [city, county]
    cols_comm = [
        ["city", "brand_id", "brand_name", "ciga_data_marker"],
        ["county", "sale_center_id", "brand_id", "brand_name", "ciga_data_marker"]
    ]
    cols = [
        ["brand_city_month_sales", "brand_city_month_orders", "brand_city_month_sales_ratio",
         "brand_city_month2_sales", "brand_city_month2_orders", "brand_city_month2_sales_ratio",
         "brand_city_month_sales_last_year", "brand_city_month_orders_last_year", "brand_city_month_retio_last_year"
         ],
        ["brand_county_month_sales", "brand_county_month_orders", "brand_county_month_sales_ratio",
         "brand_county_month2_sales", "brand_county_month2_orders", "brand_county_month2_sales_ratio",
         "brand_county_month_sales_last_year", "brand_county_month_orders_last_year",
         "brand_county_month_retio_last_year"
         ]
    ]

    for i in range(len(groups)):
        group = groups[i]
        join = joins[i]
        c = cols[i]
        marker = markers[i]
        try:
            # 获取上一个月订单行表数据
            co_co_line = get_co_co_line(spark, scope=[1, 1], filter="month") \
                .withColumn("brand_name", element_at(f.split("item_name", "\("), f.lit(1))) \
                .select("brand_name", group, "qty_ord")

            # 1、2.市/区每款烟的订单量，订单数
            print(f"{str(dt.now())}  上一个月{group}每个品牌的订单量，订单数")
            qty_amt = co_co_line.groupBy(group, "brand_name").agg(f.sum(col("qty_ord")).alias(c[0]),
                                                                  f.count(col(group)).alias(c[1]))

            # 3.市/区每款卷烟销量占比
            print(f"{str(dt.now())}  上一个月{group}每个品牌的销量占比")
            qty_ratio = co_co_line.groupBy(group) \
                .agg(f.sum(col("qty_ord")).alias("qty_ord_total")) \
                .join(qty_amt, group) \
                .withColumn(c[2], col(c[0]) / col("qty_ord_total"))

            # --------------------环比

            # 获取上第二个月订单行表数据
            co_co_line = get_co_co_line(spark, scope=[2, 2], filter="month") \
                .withColumn("brand_name", element_at(f.split("item_name", "\("), f.lit(1))) \
                .select("brand_name", group, "qty_ord")

            # 市/区每款卷烟订单量，订单数 上次同期
            qty_amt_last = co_co_line.groupBy(group, "brand_name").agg(f.sum(col("qty_ord")).alias(c[3]),
                                                                       f.count(col(group)).alias(c[4]))
            # 市/区每款卷烟销量占比 上次同期
            qty_ratio_last = co_co_line.groupBy(group) \
                .agg(f.sum(col("qty_ord")).alias("qty_ord_total_last")) \
                .join(qty_amt_last, group) \
                .withColumn(c[5], col(c[3]) / col("qty_ord_total_last"))

            # # 4、5.市/区每款卷烟订单量，订单数环比
            # print(f"{str(dt.now())}  上一个月{group}每个品牌的订单量，订单数的环比")
            # qty_amt_last.join(qty_amt, [group, "brand"]) \
            #     .withColumn("qty_ring_rate", period_udf(col("qty_ord"), col("qty_ord_last"))) \
            #     .withColumn("amt_ring_rate", period_udf(col("amount"), col("amount_last")))
            #
            # # 6.市/区每款卷烟订单量占比环比
            # print(f"{str(dt.now())}  上一个月{group}每个品牌的销量占比的环比")
            # qty_ratio_last.join(qty_ratio, [group, "brand"]) \
            #     .withColumn("qty_ratio_ring", period_udf(col("qty_ratio"), col("qty_ratio_last")))

            # ---------------------同比

            # 获取去年同期 上一月订单行表数据
            last_year = get_co_co_line(spark, scope=[13, 13], filter="month") \
                .withColumn("brand_name", element_at(f.split("item_name", "\("), f.lit(1))) \
                .select("brand_name", group, "qty_ord")

            # 市/区每款烟的订单量,订单数 去年同期
            qty_amt_ly = last_year.groupBy(group, "brand_name") \
                .agg(f.sum(col("qty_ord")).alias(c[6]), f.count(col(group)).alias(c[7]))

            # 市/区每款烟销量的占比 去年同期
            qty_ratio_ly = last_year.groupBy(group) \
                .agg(f.sum(col("qty_ord")).alias("qty_ord_total_ly")) \
                .join(qty_amt_ly, group) \
                .withColumn(c[8], col(c[6]) / col("qty_ord_total_ly"))

            # # 7、8. 市/区每款卷烟订单量,订单数 同比
            # print(f"{str(dt.now())}  上一个月{group}每个品牌的订单量，订单数的同比")
            # qty_amt_ly.join(qty_amt, [group, "brand"]) \
            #     .withColumn("qty_ord_yoy", period_udf(col("qty_ord"), col("qty_ord_ly"))) \
            #     .withColumn("amt_yoy", period_udf(col("amount"), col("amount_ly")))
            # # 9.市/区每款烟销量占比的同比
            # print(f"{str(dt.now())}  上一个月{group}每个品牌的销量占比的同比")
            # qty_ratio_ly.join(qty_ratio, [group, "brand"]) \
            #     .withColumn("qty_ratio_yoy", period_udf(col("qty_ratio"), col("qty_ratio_ly")))

            all_df = qty_amt.join(qty_ratio, [group, "brand_name"], "outer") \
                .join(qty_amt_last, [group, "brand_name"], "outer") \
                .join(qty_ratio_last, [group, "brand_name"], "outer") \
                .join(qty_amt_ly, [group, "brand_name"], "outer") \
                .join(qty_ratio_ly, [group, "brand_name"], "outer")
            column = cols_comm + c
            all_df.join(brand, "brand_name") \
                .withColumn("row", f.concat_ws("_", col(group), col("brand_id"))) \
                .join(join, group) \
                .withColumn("ciga_data_marker", f.lit(marker)) \
                .foreachPartition(lambda x: write_hbase1(x, column, hbase))
        except Exception:
            tb.print_exc()


def get_brand_stats_info_weekly():
    # 市/区每款卷烟上一周销量/环比/同比

    # 烟品牌id，烟品牌名称
    brand = get_phoenix_table(spark, "brand")

    area = get_area(spark)
    # com_id与city的映射关系
    city = area.dropDuplicate(["com_id", "sale_center_id"]).select("com_id", "city")
    # sale_center_id与区(list)的映射关系
    county = area.groupBy("sale_center_id") \
        .agg(f.collect_list("county").alias("county")) \
        .select("sale_center_id", "county")

    markers = ["0", "2"]
    # 按照 市或区统计
    groups = ["com_id", "sale_center_id"]
    joins = [city, county]
    cols_comm = [
        ["city", "brand_id", "brand_name", "ciga_data_marker"],
        ["county", "sale_center_id", "brand_id", "brand_name", "ciga_data_marker"]
    ]
    cols = [
        ["brand_city_week_sales", "brand_city_week2_sales", "brand_city_week_sales_last_year"],
        ["brand_county_week_sales", "brand_county_week2_sales", "brand_county_week_sales_last_year"]
    ]

    for i in range(len(groups)):
        group = groups[i]
        join = joins[i]
        c = cols[i]
        marker = markers[i]
        try:
            # 获取上一周订单行表数据
            co_co_line = get_co_co_line(spark, scope=[1, 1], filter="week") \
                .withColumn("brand_name", element_at(f.split("item_name", "\("), f.lit(1))) \
                .select("brand_name", group, "qty_ord")

            # 1.市/区每款烟的订单量
            print(f"{str(dt.now())}  上一周{group}每个品牌的销量")
            qty_amt = co_co_line.groupBy(group, "brand_name").agg(f.sum(col("qty_ord")).alias(c[0]))

            # -------------------环比
            # 获取上第二周订单行表数据
            co_co_line = get_co_co_line(spark, scope=[2, 2], filter="week") \
                .withColumn("brand_name", element_at(f.split("item_name", "\("), f.lit(1))) \
                .select("brand_name", group, "qty_ord")

            # 市/区每款卷烟销量 上次同期
            qty_amt_last = co_co_line.groupBy(group, "brand_name").agg(f.sum(col("qty_ord")).alias(c[1]))

            # # 2.市/区每款卷烟销量 环比
            # print(f"{str(dt.now())}  上一周{group}每个品牌的销量环比")
            # qty_amt_last.join(qty_amt, [group, "brand"]) \
            #     .withColumn("qty_ring_rate", period_udf(col("qty_ord"), col("qty_ord_last")))

            # --------------------同比
            last_year = spark.sql("select  * from DB2_DB2INST1_CO_CO_LINE") \
                .where(col("com_id").isin(cities)) \
                .withColumn("born_date", f.to_date("born_date", "yyyyMMdd")) \
                .withColumn("last_year_today", f.date_sub(f.current_date(), 365)) \
                .withColumn("week_diff", week_diff("last_year_today", "born_date")) \
                .where(col("week_diff") == 1) \
                .withColumn("qty_ord", col("qty_ord").cast("float")) \
                .withColumn("brand_name", element_at(f.split("item_name", "\("), f.lit(1))) \
                .select("brand_name", group, "qty_ord")
            # 市/区每款烟的订单量 去年
            qty_ord_ly = last_year.groupBy(group, "brand_name") \
                .agg(f.sum(col("qty_ord")).alias(c[2]))
            # # 3.市/区每款卷烟销量同比
            # print(f"{str(dt.now())}  上一周{group}每个品牌的销量的同比")
            # qty_ord_ly.join(qty_amt, [group, "brand"]) \
            #     .withColumn("qty_ord_yoy", period_udf(col("qty_ord"), col("qty_ord_ly")))

            all_df = qty_amt.join(qty_amt_last, [group, "brand_name"], "outer") \
                .join(qty_ord_ly, [group, "brand_name"], "outer")

            column = cols_comm + c
            all_df.join(brand, "brand_name") \
                .withColumn("row", f.concat_ws("_", col(group), col("brand_id"))) \
                .join(join, group) \
                .withColumn("ciga_data_marker", f.lit(marker)) \
                .foreachPartition(lambda x: write_hbase1(x, column, hbase))
        except Exception:
            tb.print_exc()


def get_item_ratio():
    # 市/区 同品牌各规格销量占比

    # 烟品牌id，烟品牌名称
    brand = get_phoenix_table(spark, "brand")
    # 烟id，烟名称
    plm_item = get_plm_item(spark).select("item_id", "item_name")

    area = get_area(spark)
    # com_id与city的映射关系
    city = area.dropDuplicate(["com_id", "sale_center_id"]).select("com_id", "city")
    # sale_center_id与区(list)的映射关系
    county = area.groupBy("sale_center_id") \
        .agg(f.collect_list("county").alias("county")) \
        .select("sale_center_id", "county")

    markers = ["1", "3"]
    # 按照 市或区统计
    groups = ["com_id", "sale_center_id"]
    joins = [city, county]
    cols_comm = [
        ["city", "brand_id", "brand_name", "ciga_id", "ciga_gauge", "ciga_data_marker"],
        ["county", "sale_center_id", "brand_id", "brand_name", "ciga_id", "ciga_gauge", "ciga_data_marker"]
    ]
    cols = [
        "city_brand_gauge_sales_ratio",
        "county_brand_gauge_sales_ratio"
    ]

    for i in range(len(groups)):
        group = groups[i]
        join = joins[i]
        c = cols[i]
        marker = markers[i]
        try:
            # 上个月同品牌各规格销量占比
            co_co_line = get_co_co_line(spark, scope=[1, 1], filter="month") \
                .withColumn("brand_name", element_at(f.split("item_name", "\("), f.lit(1))) \
                .select("item_id", "brand_name", "qty_ord", group)

            # 各市/区 各品牌销量
            brand_ord = co_co_line.groupBy(group, "brand_name").agg(f.sum(col("qty_ord")).alias("brand_ord"))
            # 各市/区 每个品牌各品规销量
            item_ord = co_co_line.groupBy(group, "brand_name", "item_id").agg(f.sum(col("qty_ord")).alias("item_ord"))
            # 销量占比
            print(f"{str(dt.now())}  上一个月同品牌各品规的销量占比")
            brand_item = brand_ord.join(item_ord, [group, "brand"]) \
                .withColumn(c, col("item_ord") / col("brand_ord"))

            column = cols_comm + c
            brand_item.withColumn("row", f.concat_ws("_", col(group), col("item_id"))) \
                .join(plm_item, "item_id") \
                .join(brand, "brand_name") \
                .join(join, group) \
                .withColumn("ciga_data_marker", f.lit(marker)) \
                .withColumnRenamed("item_id", "ciga_id") \
                .withColumnRenamed("item_name", "ciga_gauge") \
                .foreachPartition(lambda x: write_hbase1(x, column, hbase))
        except Exception:
            tb.print_exc()


# 品规统计指标


def get_item_stats_info_daily():
    # 本月当前本市/区每款卷烟订货量
    # 本月当前本市/区每款卷烟订单额

    # 烟id，烟名称
    plm_item = get_plm_item(spark).select("item_id", "item_name")

    area = get_area(spark)
    # com_id与city的映射关系
    city = area.dropDuplicate(["com_id", "sale_center_id"]).select("com_id", "city")
    # sale_center_id与区(list)的映射关系
    county = area.groupBy("sale_center_id") \
        .agg(f.collect_list("county").alias("county")) \
        .select("sale_center_id", "county")
    markers = ["1", "3"]
    # 按照 市或区统计
    groups = ["com_id", "sale_center_id"]
    joins = [city, county]
    cols_comm = [["city", "ciga_id", "ciga_gauge", "ciga_data_marker"],
                 [ "county", "sale_center_id", "ciga_id", "ciga_gauge", "ciga_data_marker"]]
    cols = [["gauge_city_orders", "gauge_city_order_amount"], ["gauge_county_orders", "gauge_county_order_amount"]]
    for i in range(len(groups)):
        group = groups[i]
        join = joins[i]
        c = cols[i]
        marker = markers[i]
        try:
            # 获取本月截止订单行表数据
            co_co_line = get_co_co_line(spark, scope=[0, 0], filter="month") \
                .select("item_id", "qty_ord", "amt", group)
            # 本月每款烟在每个区的订单量,订单额
            # com_id item_id qty_ord amt
            qty_amt = co_co_line.groupBy([group, "item_id"]) \
                .agg(f.sum(col("qty_ord")).alias(c[0]), f.sum(col("amt")).alias(c[1]))

            column = cols_comm[i] + c
            qty_amt.withColumn("row", f.concat_ws("_", col(group), col("item_id"))) \
                .join(plm_item, "item_id") \
                .join(join, group) \
                .withColumnRenamed("item_id", "ciga_id") \
                .withColumnRenamed("item_name", "ciga_gauge") \
                .withColumn("ciga_data_marker", f.lit(marker)) \
                .foreachPartition(lambda x: write_hbase1(x, column, hbase))
        except Exception:
            tb.print_exc()


def get_item_stats_info_monthly():
    # 市/区每款卷烟上一月销量/环比/同比
    # 市/区每款卷烟上一月订单数/环比/同比
    # 市/区每款卷烟上一月销量占比/环比/同比

    # 烟id，烟名称
    plm_item = get_plm_item(spark).select("item_id", "item_name")

    area = get_area(spark)
    # com_id与city的映射关系
    city = area.dropDuplicate(["com_id", "sale_center_id"]).select("com_id", "city")
    # sale_center_id与区(list)的映射关系
    county = area.groupBy("sale_center_id") \
        .agg(f.collect_list("county").alias("county")) \
        .select("sale_center_id", "county")

    markers = ["1", "3"]
    # 按照 市或区统计
    groups = ["com_id", "sale_center_id"]
    joins = [city, county]
    cols_comm = [
        [ "city", "ciga_id", "ciga_gauge", "ciga_data_marker"],
        [ "county", "sale_center_id", "ciga_id", "ciga_gauge", "ciga_data_marker"]
    ]
    cols = [
        ["gauge_city_month_sales", "gauge_city_month_orders", "gauge_city_month_sales_ratio",
         "gauge_city_month2_sales", "gauge_city_month2_orders", "gauge_city_month2_sales_ratio",
         "gauge_city_month_sales_last_year", "gauge_city_month_orders_last_year", "gauge_city_month_retio_last_year"
         ],
        ["gauge_county_month_sales", "gauge_county_month_orders", "gauge_county_month_sales_ratio",
         "gauge_county_month2_sales", "gauge_county_month2_orders", "gauge_county_month2_sales_ratio",
         "gauge_county_month_sales_last_year", "gauge_county_month_orders_last_year",
         "gauge_county_month_retio_last_year"
         ]
    ]

    for i in range(len(groups)):
        group = groups[i]
        join = joins[i]
        c = cols[i]
        marker = markers[i]
        try:
            # 获取上一个月订单行表数据
            co_co_line = get_co_co_line(spark, scope=[1, 1], filter="month") \
                .select("item_id", group, "qty_ord")

            # 1、2.市/区每款烟的订单量，订单数
            print(f"{str(dt.now())}  上一个月{group}每款烟的订单量，订单数")
            qty_amt = co_co_line.groupBy(group, "item_id") \
                .agg(f.sum(col("qty_ord")).alias(c[0]), f.count(col(group)).alias(c[1]))

            # 3.市/区每款卷烟销量占比
            print(f"{str(dt.now())}  上一个月{group}每款烟的销量占比")
            qty_ratio = co_co_line.groupBy(group) \
                .agg(f.sum(col("qty_ord")).alias("qty_ord_total")) \
                .join(qty_amt, group) \
                .withColumn(c[2], col(c[0]) / col("qty_ord_total"))

            # -----------------------------------------------------环比
            # 先计算上第二个月 的订单量，订单数 ，销量占比，再join 上个月的
            # 获取上第二个月订单行表数据
            co_co_line = get_co_co_line(spark, scope=[2, 2], filter="month") \
                .select("item_id", group, "qty_ord")

            # 市/区每款卷烟订单量，订单数 上次同期
            qty_amt_last = co_co_line.groupBy(group, "item_id") \
                .agg(f.sum(col("qty_ord")).alias(c[3]), f.count(col(group)).alias(c[4]))
            # 市/区每款卷烟销量占比 上次同期
            qty_ratio_last = co_co_line.groupBy(group) \
                .agg(f.sum(col("qty_ord")).alias("qty_ord_total_last")) \
                .join(qty_amt_last, group) \
                .withColumn(c[5], col(c[3]) / col("qty_ord_total_last"))

            # # 4、5.市/区每款卷烟订单量，订单额环比
            # print(f"{str(dt.now())}  上一个月{group}每款烟的订单量，订单数环比")
            # qty_amt_last.join(qty_amt, [group, "item_id"]) \
            #     .withColumn("qty_ring_rate", period_udf(col("qty_ord"), col("qty_ord_last"))) \
            #     .withColumn("amt_ring_rate", period_udf(col("amount"), col("amount_last")))
            #
            # # 6.市/区每款卷烟订单量占比环比
            # print(f"{str(dt.now())}  上一个月{group}每款烟的销量占比的环比")
            # qty_ratio_last.join(qty_ratio, [group, "item_id"]) \
            #     .withColumn("qty_ratio_ring", period_udf(col("qty_ratio"), col("qty_ratio_last")))

            # -----------------------------------------------------同比
            # 先计算全年同期 的订单量，订单数 ，销量占比，再join今年的
            # 获取去年同期 上一月订单行表数据
            last_year = get_co_co_line(spark, scope=[13, 13], filter="month") \
                .select("item_id", group, "qty_ord")

            # 市/区每款烟的订单量,订单数 去年同期
            qty_amt_ly = last_year.groupBy(group, "item_id") \
                .agg(f.sum(col("qty_ord")).alias(c[6]), f.count(col(group)).alias(c[7]))

            # 市/区每款烟的占比 去年同期
            qty_ratio_ly = last_year.groupBy(group) \
                .agg(f.sum(col("qty_ord")).alias("qty_ord_total")) \
                .join(qty_amt_ly, group) \
                .withColumn(c[8], col(c[6]) / col("qty_ord_total"))

            # # 7、8. 市/区每款卷烟订单量,订单数 同比
            # print(f"{str(dt.now())}  上一个月{group}每款烟的订单量，订单数同比")
            # qty_amt_ly.join(qty_amt, [group, "item_id"]) \
            #     .withColumn("qty_ord_yoy", period_udf(col("qty_ord"), col("qty_ord_ly"))) \
            #     .withColumn("amt_yoy", period_udf(col("amount"), col("amount_ly")))
            # # 9.市/区每款烟销量占比的同比
            # print(f"{str(dt.now())}  上一个月{group}每款烟的销量占比的同比")
            # qty_ratio_ly.join(qty_ratio, [group, "item_id"]) \
            #     .withColumn("qty_ratio_yoy", period_udf(col("qty_ratio"), col("qty_ratio_ly")))

            all_df = qty_amt.join(qty_ratio, [group, "item_id"], "outer") \
                .join(qty_amt_last, [group, "item_id"], "outer") \
                .join(qty_ratio_last, [group, "item_id"], "outer") \
                .join(qty_amt_ly, [group, "item_id"], "outer") \
                .join(qty_ratio_ly, [group, "item_id"], "outer")
            column = cols_comm + c
            all_df.withColumn("row", f.concat_ws("_", col(group), col("item_id"))) \
                .join(plm_item, "item_id") \
                .join(join, group) \
                .withColumnRenamed("item_id", "ciga_id") \
                .withColumnRenamed("item_name", "ciga_gauge") \
                .withColumn("ciga_data_marker", f.lit(marker)) \
                .foreachPartition(lambda x: write_hbase1(x, column, hbase))
        except Exception:
            tb.print_exc()


def get_item_stats_info_weekly():
    # 市/区每款卷烟上一周销量/环比/同比
    # 烟id，烟名称
    plm_item = get_plm_item(spark).select("item_id", "item_name")

    area = get_area(spark)
    # com_id与city的映射关系
    city = area.dropDuplicate(["com_id", "sale_center_id"]).select("com_id", "city")
    # sale_center_id与区(list)的映射关系
    county = area.groupBy("sale_center_id") \
        .agg(f.collect_list("county").alias("county")) \
        .select("sale_center_id", "county")

    markers = ["1", "3"]
    # 按照 市或区统计
    groups = ["com_id", "sale_center_id"]
    joins = [city, county]
    cols_comm = [
        [ "city", "ciga_id", "ciga_gauge", "ciga_data_marker"],
        [ "county", "sale_center_id", "ciga_id", "ciga_gauge", "ciga_data_marker"]
    ]
    cols = [
        ["gauge_city_week_sales", "gauge_city_week_sales_last_year", "gauge_city_week2_sales"],
        ["gauge_county_week_sales", "gauge_county_week_sales_last_year", "gauge_county_week2_sales"]
    ]
    for i in range(len(groups)):
        group = groups[i]
        join = joins[i]
        c = cols[i]
        marker = markers[i]
        try:
            # 获取上一周订单行表数据
            co_co_line = get_co_co_line(spark, scope=[1, 1], filter="week") \
                .select("item_id", group, "qty_ord")

            # 1.市/区每款烟的订单量
            print(f"{str(dt.now())}  上一周{group}每款烟的订单量")
            qty_amt = co_co_line.groupBy(group, "item_id").agg(f.sum(col("qty_ord")).alias(c[0]))

            # -------------------环比
            # 获取上第二周订单行表数据
            co_co_line = get_co_co_line(spark, scope=[2, 2], filter="week") \
                .select("item_id", group, "qty_ord")

            # 市/区每款卷烟销量 上次同期
            qty_amt_last = co_co_line.groupBy(group, "item_id").agg(f.sum(col("qty_ord")).alias(c[1]))

            # # 2.市/区每款卷烟销量 环比
            # print(f"{str(dt.now())}  上一周{group}每款烟销量环比")
            # qty_amt_last.join(qty_amt, [group, "item_id"]) \
            #     .withColumn("qty_ring_rate", period_udf(col("qty_ord"), col("qty_ord_last")))

            # --------------------同比
            last_year = spark.sql("select  * from DB2_DB2INST1_CO_CO_LINE") \
                .where(col("com_id").isin(cities)) \
                .withColumn("born_date", f.to_date("born_date", "yyyyMMdd")) \
                .withColumn("last_year_today", f.date_sub(f.current_date(), 365)) \
                .withColumn("week_diff", week_diff("last_year_today", "born_date")) \
                .where(col("week_diff") == 1) \
                .withColumn("qty_ord", col("qty_ord").cast("float")) \
                .select(group, "item_id", "qty_ord")
            # 市/区每款烟的订单量 去年
            qty_ord_ly = last_year.groupBy(group, "item_id") \
                .agg(f.sum(col("qty_ord")).alias(c[2]))

            # #3 市/区每款卷烟销量同比
            # print(f"{str(dt.now())}  上一周{group}每款烟销量同比")
            # qty_ord_ly.join(qty_amt, [group, "item_id"]) \
            #     .withColumn("qty_ord_yoy", period_udf(col("qty_ord"), col("qty_ord_ly")))

            all_df = qty_amt.join(qty_amt_last, [group, "item_id"], "outer") \
                .join(qty_ord_ly, [group, "item_id"], "outer")

            column = cols_comm + c
            all_df.withColumn("row", f.concat_ws("_", col(group), col("item_id"))) \
                .join(plm_item, "item_id") \
                .join(join, group) \
                .withColumnRenamed("item_id", "ciga_id") \
                .withColumnRenamed("item_name", "ciga_gauge") \
                .withColumn("ciga_data_marker", f.lit(marker)) \
                .foreachPartition(lambda x: write_hbase1(x, column, hbase))
        except Exception:
            tb.print_exc()


def get_cover_rate():
    # 店铺覆盖率

    # 烟id，烟名称
    plm_item = get_plm_item(spark).select("item_id", "item_name")

    area = get_area(spark)
    # com_id与city的映射关系
    city = area.dropDuplicate(["com_id", "sale_center_id"]).select("com_id", "city")
    # sale_center_id与区(list)的映射关系
    county = area.groupBy("sale_center_id") \
        .agg(f.collect_list("county").alias("county")) \
        .select("sale_center_id", "county")

    markers = ["1", "3"]
    # 按照 市或区统计
    groups = ["com_id", "sale_center_id"]
    joins = [city, county]
    cols_comm = [
        [ "city", "ciga_id", "ciga_gauge", "ciga_data_marker"],
        [ "county", "sale_center_id", "ciga_id", "ciga_gauge", "ciga_data_marker"]
    ]
    cols = [
        "city_gauge_retail_ratio",
        "county_gauge_retail_ratio"
    ]

    for i in range(len(groups)):
        group = groups[i]
        join = joins[i]
        c = cols[i]
        marker = markers[i]
        try:
            co_cust = get_co_cust(spark).select("cust_id", group)
            co_co_line = get_co_co_line(spark, scope=[1, 1], filter="month") \
                .select("item_id", "cust_id", group)

            print(f"{str(dt.now())}  卷烟店铺覆盖率  {area}级别")

            # 每个区域零售户数量
            cust_num = co_cust.groupBy(group).agg(f.count("cust_id").alias("cust_num"))
            # 每个区域每款卷烟覆盖店面数量
            item_cover_num = co_co_line.dropDuplicates(["cust_id", "item_id"]) \
                .groupBy(group, "item_id") \
                .agg(f.count("cust_id").alias("item_cover_num"))

            cover_ratio = item_cover_num.join(cust_num, group) \
                .withColumn(c, col("item_cover_num") / col("cust_num"))

            column = cols_comm[i] + c
            cover_ratio.withColumn("row", f.concat_ws("_", col(group), col("item_id"))) \
                .join(plm_item, "item_id") \
                .join(join, group) \
                .withColumn("ciga_data_marker", f.lit(marker)) \
                .withColumnRenamed("item_id", "ciga_id") \
                .withColumnRenamed("item_name", "ciga_gauge") \
                .foreachPartition(lambda x: write_hbase1(x, column, hbase))
        except Exception:
            tb.print_exc()