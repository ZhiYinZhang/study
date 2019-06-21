#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/6/11 10:49
import traceback as tb
from pyspark.sql.functions import col
from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from pyspark import SparkContext
from datetime import datetime as dt
from pyspark.sql import Window
import json
from application.tobacco_rules.rules.write_hbase import write_hbase1
from application.tobacco_rules.rules.utils import *
from application.tobacco_rules.rules.config import goods_supply_table

spark = SparkSession.builder \
    .enableHiveSupport() \
    .appName("goods supply") \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

spark.sql("use aistrong")

hbase = {"table": goods_supply_table, "families": ["0"], "row": "row"}


def reality_supply():
    # 全市上周该品规计划投放量
    # 全市上周该品规实际投放量
    # 全市上周该品规投放剩余量
    try:
        print(f"{str(dt.now())} 全市上周各品规 计划/实际/剩余 量")
        plm_item = get_plm_item(spark).select("item_id", "item_name")

        area = get_area(spark)
        # com_id与city的映射关系
        city = area.dropDuplicate(["com_id", "sale_center_id"]).select("com_id", "city")

        # 获取上周
        cust_item_spw = spark.sql(
            "select com_id,item_id,qty_plan,qty_remain,qty_allocco,begin_date,end_date from DB2_DB2INST1_SGP_CUST_ITEM_SPW") \
            .withColumn("begin_date", f.to_date(col("begin_date"), "yyyyMMdd")) \
            .withColumn("end_date", f.to_date(col("end_date"), "yyyyMMdd")) \
            .withColumn("last_mon", f.date_sub(f.date_trunc("week", f.current_date()), 7)) \
            .withColumn("last_sun", f.date_add(col("last_mon"), 6)) \
            .where((col("begin_date") == col("last_mon")) & (col("end_date") == col("last_sun")))

        #需要计算的值的列名
        cols = ["gauge_week_planned_volume", "gauge_week_reality_volume", "gauge_week_residue_volume"]
        result = cust_item_spw.groupBy("com_id", "item_id") \
            .agg(f.sum("qty_plan").alias(cols[0]),
                 f.sum("qty_allocco").alias(cols[1]),
                 f.sum("qty_remain").alias(cols[2]))

        columns = ["gauge_id", "gauge_name", "city", "gears_date_marker"] + cols
        result.withColumn("row", f.concat_ws("_", col("com_id"), col("item_id"))) \
            .withColumn("gears_data_marker", f.lit("0")) \
            .join(plm_item, "item_id") \
            .join(city, "com_id") \
            .withColumnRenamed("item_id","gauge_id")\
             .withColumnRenamed("item_name","gauge_name")\
            .foreachPartition(lambda x: write_hbase1(x, columns, hbase))
    except Exception:
        tb.print_exc()

def supply_analysis_city():
    # 全市上周该品规订足率
    # 全市订足率最低5款卷烟
    # 全市上周该品规订足面
    # 全市上周该品规进货面
    # 全市上周该品规重购率
    # 全市各零售户该品规上周订足率热力分布
    try:
        plm_item = get_plm_item(spark).select("item_id", "item_name")

        area = get_area(spark)
        # com_id与city的映射关系
        city = area.dropDuplicate(["com_id", "sale_center_id"]).select("com_id", "city")

        co_co_line = get_co_co_line(spark, scope=[1, 1], filter="week") \
            .select("com_id", "cust_id", "item_id", "qty_ord", "qty_rsn")

        try:
            print(f"{str(dt.now())} 全市上周该品规订足率")
            colName = "gauge_week_reserve_ratio"
            reserve_ratio = co_co_line.groupBy("com_id", "item_id") \
                .agg(f.sum("qty_ord").alias("qty_ord"), f.sum("qty_rsn").alias("qty_rsn")) \
                .withColumn(colName, col("qty_ord") / col("qty_rsn"))

            columns = ["city", "gauge_id", "gauge_name", "gears_data_marker", colName]
            reserve_ratio.join(city, "com_id") \
                .join(plm_item, "item_id") \
                .withColumn("row", f.concat_ws("_", col("com_id"), col("item_id"))) \
                .withColumn("gears_data_marker", f.lit("0")) \
                .withColumnRenamed("item_id", "gauge_id") \
                .withColumnRenamed("item_name", "gauge_name") \
                .foreachPartition(lambda x: write_hbase1(x, columns, hbase))

            print(f"{str(dt.now())}  全市订足率最低5款卷烟")
            win = Window.partitionBy("com_id").orderBy(colName)
            json_udf = f.udf(lambda x, y,z: json.dumps({"gaugeId":x,"gaugeName":y,"gaugeReserve": z}))
            colName = "reserve_min_ciga5_name"
            top5 = reserve_ratio.withColumn("rank", f.row_number().over(win)) \
                .where(col("rank") <= 5) \
                .join(plm_item, "item_id") \
                .withColumn("json", json_udf(col("item_id"), col("item_name"),col("gauge_week_reserve_ratio"))) \
                .groupBy("com_id") \
                .agg(f.collect_list(col("json")).alias(colName))
            columns = ["city", "gears_data_marker", colName]
            top5.join(city, "com_id") \
                .withColumnRenamed("com_id", "row") \
                .withColumn("gears_data_marker", f.lit("3")) \
                .foreachPartition(lambda x: write_hbase1(x, columns, hbase))
        except Exception:
            tb.print_exc()
        try:
            print(f"{str(dt.now())} 全市上周该品规订足面")
            colName = "gauge_week_reserve_face"
            total = co_co_line.groupBy("com_id", "item_id") \
                .agg(f.count(col("item_id")).alias("total"))
            reserve_face = co_co_line.where(col("qty_ord") == col("qty_rsn")) \
                .groupBy("com_id", "item_id") \
                .agg(f.count(col("item_id")).alias("enough_count")) \
                .join(total, ["com_id", "item_id"]) \
                .withColumn(colName, col("enough_count") / col("total"))

            columns = ["city", "gauge_id", "gauge_name", "gears_data_marker", colName]
            reserve_face.join(city, "com_id") \
                .join(plm_item, "item_id") \
                .withColumn("row", f.concat_ws("_", col("com_id"), col("item_id"))) \
                .withColumn("gears_data_marker", f.lit("0")) \
                .withColumnRenamed("item_id", "gauge_id") \
                .withColumnRenamed("item_name", "gauge_name") \
                .foreachPartition(lambda x: write_hbase1(x, columns, hbase))

        except Exception:
            tb.print_exc()

        try:
            # 该款烟订单>0的数量/该款烟总的订单数量
            colName = "gauge_week_stock_face"
            print(f"{str(dt.now())} 全市上周该品规进货面")
            stock_face = co_co_line.where(col("qty_ord") > 0) \
                .groupBy("com_id", "item_id") \
                .agg(f.count(col("item_id")).alias("stock_count")) \
                .join(total, ["com_id", "item_id"]) \
                .withColumn(colName, col("stock_count") / col("total"))

            columns = ["city", "gauge_id", "gauge_name", "gears_data_marker", colName]
            stock_face.join(city, "com_id") \
                .join(plm_item, "item_id") \
                .withColumn("row", f.concat_ws("_", col("com_id"), col("item_id"))) \
                .withColumn("gears_data_marker", f.lit("0")) \
                .withColumnRenamed("item_id", "gauge_id") \
                .withColumnRenamed("item_name", "gauge_name") \
                .foreachPartition(lambda x: write_hbase1(x, columns, hbase))
        except Exception:
            tb.print_exc()

        try:
            print(f"{str(dt.now())} 全市上周该品规重购率")
            # 上上周
            last_two_week = get_co_co_line(spark, scope=[2, 2], filter="week").select("com_id", "cust_id", "item_id",
                                                                                      "qty_ord")

            # 上上周哪些零售户订了这款烟
            last_two_item = last_two_week.where(col("qty_ord") > 0) \
                .dropDuplicates(["cust_id", "item_id"])
            # 上周哪些零售户订了这款烟
            item = co_co_line.where(col("qty_ord") > 0) \
                .dropDuplicates(["cust_id", "item_id"])

            # 上周和上上周哪些零售户都定了这款烟 并统计重购的零售户数量
            reorder_num = last_two_item.join(item, ["cust_id", "item_id"]) \
                .groupBy("com_id", "item_id") \
                .agg(f.count("item_id").alias("reorder_num"))
            colName = "gauge_week_again"
            reorder_ratio = item.groupBy("com_id", "item_id") \
                .agg(f.count("item_id").alias("total_num")) \
                .join(reorder_num, ["com_id", "item_id"]) \
                .withColumn(colName, col("reorder_num") / col("total_num"))

            columns = ["city", "gauge_id", "gauge_name", "gears_data_marker", colName]
            reorder_ratio.join(city, "com_id") \
                .join(plm_item, "item_id") \
                .withColumn("row", f.concat_ws("_", col("com_id"), col("item_id"))) \
                .withColumn("gears_data_marker", f.lit("0")) \
                .withColumnRenamed("item_id", "gauge_id") \
                .withColumnRenamed("item_name", "gauge_name") \
                .foreachPartition(lambda x: write_hbase1(x, columns, hbase))
        except Exception:
            tb.print_exc()

        try:
            colName = "retail_gauge_week_reserve"
            # 零售户经纬度
            co_cust = get_co_cust(spark).select("cust_id", "cust_name") \
                .join(get_cust_lng_lat(spark), "cust_id") \
                .withColumnRenamed("lng", "longitude") \
                .withColumnRenamed("lat", "latitude")

            print(f"{str(dt.now())} 全市各零售户该品规上周订足率热力分布")
            enouth_ratio = co_co_line.groupBy("com_id", "cust_id", "item_id") \
                .agg(f.sum(col("qty_ord")).alias("qty_ord"), f.sum(col("qty_rsn")).alias("qty_rsn")) \
                .withColumn(colName, col("qty_ord") / col("qty_rsn"))

            columns = ["city", "cust_id", "cust_name", "longitude", "latitude", "gauge_id", "gauge_name",
                       "gears_data_marker", colName]
            enouth_ratio.join(city, "com_id") \
                .join(co_cust, "cust_id") \
                .join(plm_item, "item_id") \
                .withColumn("row", f.concat_ws("_", col("cust_id"), col("item_id"))) \
                .withColumn("gears_data_marker", f.lit("2")) \
                .withColumnRenamed("item_id", "gauge_id") \
                .withColumnRenamed("item_name", "gauge_name") \
                .foreachPartition(lambda x: write_hbase1(x, columns, hbase))
        except Exception:
            tb.print_exc()
    except Exception:
        tb.print_exc()

def supply_analysis_county():
    # 各区县该卷烟品规订足率
    plm_item = get_plm_item(spark).select("item_id", "item_name")

    area = get_area(spark)

    # sale_center_id与区(list)的映射关系
    county = area.groupBy("sale_center_id") \
        .agg(f.collect_list("county").alias("county")) \
        .select("sale_center_id", "county")

    co_co_line = get_co_co_line(spark, scope=[1, 1], filter="week") \
        .select("sale_center_id", "item_id", "qty_ord", "qty_rsn")

    try:
        print(f"{str(dt.now())} 各区县该卷烟品规订足率")
        colName = "county_gauge_reserve"
        reserve_ratio = co_co_line.groupBy("sale_center_id", "item_id") \
            .agg(f.sum("qty_ord").alias("qty_ord"), f.sum("qty_rsn").alias("qty_rsn")) \
            .withColumn(colName, col("qty_ord") / col("qty_rsn"))

        columns = ["sale_center_id", "county", "gauge_id", "gauge_name", "gears_data_marker", colName]
        reserve_ratio.join(county, "sale_center_id") \
            .join(plm_item, "item_id") \
            .withColumn("row", f.concat_ws("_", col("sale_center_id"), col("item_id"))) \
            .withColumn("gears_data_marker", f.lit("1")) \
            .withColumnRenamed("item_id", "gauge_id") \
            .withColumnRenamed("item_name", "gauge_name") \
            .foreachPartition(lambda x: write_hbase1(x, columns, hbase))
    except Exception:
        tb.print_exc()