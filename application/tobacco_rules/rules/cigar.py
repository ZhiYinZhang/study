#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/6/19 10:21
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as f
from pyspark.sql import Window
from datetime import datetime as dt
import traceback as tb
import json
from rules.utils import get_co_co_line,week_diff,element_at,get_plm_item,\
    get_area,get_phoenix_table,get_co_cust,get_cust_lng_lat,get_rating,item_name_udf,get_cigar_rating
from rules.config import cities,als_table,ciga_table,brand_table
from rules.write_hbase import write_hbase1
from ml.similar_cigar import get_similar_cigar
from ml.client_similar_cigar import get_client_similar_cigar
from ml.forecast_qtyord.predict_sales import predict


spark=SparkSession.builder.enableHiveSupport().appName("cigar").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.sql("use aistrong")


hbase={"table":ciga_table+"_TEMP","families":["0"],"row":"row"}

#-----------------------品牌统计指标

def get_brand_stats_info_daily():
    #本月当前本市/区各品牌订货量
    #本月当前本市/区各品牌订单额
    try:
        # 烟品牌id，烟品牌名称
        brand=get_phoenix_table(spark,brand_table)
        # 烟id，烟名称
        plm_item = get_plm_item(spark).select("item_id", "item_name")

        area = get_area(spark)
        # com_id与city的映射关系
        city = area.dropDuplicates(["com_id"]).select("com_id", "city")
        # sale_center_id与区(list)的映射关系
        county = area.groupBy("sale_center_id") \
                     .agg(f.collect_list("county").alias("county")) \
                     .select("sale_center_id", "county")

        # 标识列的值
        markers = ["0", "2"]
        # 按照 市或区统计
        groups = ["com_id", "sale_center_id"]
        joins = [city, county]
        #除需要计算的值，其他的数据
        cols_comm = [["city", "brand_id", "brand_name", "ciga_data_marker"],
                     [ "county", "sale_center_id", "brand_id", "brand_name", "ciga_data_marker"]]
        #需要计算的值的列名
        cols = [["brand_city_orders", "brand_city_order_amount"], ["brand_county_orders", "brand_county_order_amount"]]

        # 1.获取本月截止订单行表数据
        co_co_line = get_co_co_line(spark, scope=[0, 0], filter="month")\
                        .join(plm_item, "item_id") \
                        .withColumn("brand_name", item_name_udf(col("item_name"))) \
                        .select("qty_ord", "amt", "brand_name", "com_id","sale_center_id")
        co_co_line.cache()

        for i in range(len(groups)):
            group = groups[i]
            join = joins[i]
            c = cols[i]
            marker = markers[i]
            try:
                print(f"{str(dt.now())}  本月{group}各品牌的订单量，订单额")

                #2.本月每款烟在每个区的订单量,订单额
                qty_amt=co_co_line.groupBy([group,"brand_name"])\
                                .agg(f.sum(col("qty_ord")).alias(c[0]),f.sum(col("amt")).alias(c[1]))

                columns = cols_comm[i] + c
                qty_amt.join(brand, "brand_name") \
                    .withColumn("row", f.concat_ws("_", col(group), col("brand_id"))) \
                    .join(join, group) \
                    .withColumn("ciga_data_marker", f.lit(marker)) \
                    .foreachPartition(lambda x: write_hbase1(x, columns, hbase))
            except Exception:
                tb.print_exc()
        co_co_line.unpersist()
    except Exception:
        tb.print_exc()




def get_brand_stats_info_monthly():
    # 市/区每款卷烟上一月销量/环比/同比
    # 市/区每款卷烟上一月订单数/环比/同比
    # 市/区每款卷烟上一月销量占比/环比/同比

    try:
        # 烟品牌id，烟品牌名称
        brand = get_phoenix_table(spark, brand_table)
        # 烟id，烟名称
        plm_item = get_plm_item(spark).select("item_id", "item_name")


        area = get_area(spark)
        # com_id与city的映射关系
        city = area.dropDuplicates(["com_id"]).select("com_id", "city")
        # sale_center_id与区(list)的映射关系
        county = area.groupBy("sale_center_id") \
            .agg(f.collect_list("county").alias("county")) \
            .select("sale_center_id", "county")

        # 标识列的值
        markers = ["0", "2"]
        # 按照 市或区统计
        groups = ["com_id", "sale_center_id"]
        joins = [city, county]
        # 除需要计算的值，其他的数据
        cols_comm = [
            ["city", "brand_id", "brand_name", "ciga_data_marker"],
            ["county", "sale_center_id", "brand_id", "brand_name", "ciga_data_marker"]
        ]
        #需要计算的值的列名
        cols = [
            ["brand_city_month_sales","brand_city_month_orders","brand_city_month_sales_ratio",
             "brand_city_month2_sales","brand_city_month2_orders","brand_city_month2_sales_ratio",
             "brand_city_month_sales_last_year","brand_city_month_orders_last_year","brand_city_month_retio_last_year"
             ],
            ["brand_county_month_sales","brand_county_month_orders","brand_county_month_sales_ratio",
             "brand_county_month2_sales","brand_county_month2_orders","brand_county_month2_sales_ratio",
             "brand_county_month_sales_last_year","brand_county_month_orders_last_year","brand_county_month_retio_last_year"
             ]
        ]


        co_co_line=get_co_co_line(spark, scope=[1, 13], filter="month") \
            .where(col("month_diff").isin([1,2,13]))\
            .join(plm_item, "item_id") \
            .withColumn("brand_name", item_name_udf(col("item_name"))) \
            .select("brand_name", "com_id", "sale_center_id", "qty_ord","month_diff")

        co_co_line.cache()

        for i in range(len(groups)):
            group = groups[i]
            join = joins[i]
            c = cols[i]
            marker=markers[i]
            try:
                # 获取上一个月订单行表数据
                last_month=co_co_line.where(col("month_diff")==1)

                # 1、2.市/区每款烟的订单量，订单数
                print(f"{str(dt.now())}  上一个月{group}每个品牌的订单量，订单数")
                qty_amt = last_month.groupBy(group, "brand_name").agg(f.sum(col("qty_ord")).alias(c[0]),
                                                                 f.count(col(group)).alias(c[1]))

                # 3.市/区每款卷烟销量占比
                print(f"{str(dt.now())}  上一个月{group}每个品牌的销量占比")
                qty_ratio = last_month.groupBy(group) \
                    .agg(f.sum(col("qty_ord")).alias("qty_ord_total")) \
                    .join(qty_amt, group) \
                    .withColumn(c[2], col(c[0]) / col("qty_ord_total"))\
                    .select(group,"brand_name",c[2])

                # --------------------环比

                # 获取上第二个月订单行表数据
                last_two_month=co_co_line.where(col("month_diff")==2)

                # 市/区每款卷烟订单量，订单数 上次同期
                print(f"{str(dt.now())}  上第二个月{group}每个品牌的订单量，订单数")
                qty_amt_last = last_two_month.groupBy(group, "brand_name").agg(f.sum(col("qty_ord")).alias(c[3]),
                                                                      f.count(col(group)).alias(c[4]))
                # 市/区每款卷烟销量占比 上次同期
                print(f"{str(dt.now())}  上第二个月{group}每个品牌的销量占比")
                qty_ratio_last = last_two_month.groupBy(group) \
                    .agg(f.sum(col("qty_ord")).alias("qty_ord_total_last")) \
                    .join(qty_amt_last, group) \
                    .withColumn(c[5], col(c[3]) / col("qty_ord_total_last"))\
                    .select(group,"brand_name",c[5])

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
                last_year = co_co_line.where(col("month_diff") == 13)

                # 市/区每款烟的订单量,订单数 去年同期
                print(f"{str(dt.now())}  去年同期{group}每个品牌的订单量，订单数")
                qty_amt_ly = last_year.groupBy(group, "brand_name") \
                    .agg(f.sum(col("qty_ord")).alias(c[6]), f.count(col(group)).alias(c[7]))

                # 市/区每款烟销量的占比 去年同期
                print(f"{str(dt.now())}  去年同期{group}每个品牌的销量占比")
                qty_ratio_ly = last_year.groupBy(group) \
                    .agg(f.sum(col("qty_ord")).alias("qty_ord_total_ly")) \
                    .join(qty_amt_ly, group) \
                    .withColumn(c[8], col(c[6]) / col("qty_ord_total_ly"))\
                    .select(group,"brand_name",c[8])

                # # 7、8. 市/区每款卷烟订单量,订单数 同比
                # print(f"{str(dt.now())}  上一个月{group}每个品牌的订单量，订单数的同比")
                # qty_amt_ly.join(qty_amt, [group, "brand"]) \
                #     .withColumn("qty_ord_yoy", period_udf(col("qty_ord"), col("qty_ord_ly"))) \
                #     .withColumn("amt_yoy", period_udf(col("amount"), col("amount_ly")))
                # # 9.市/区每款烟销量占比的同比
                # print(f"{str(dt.now())}  上一个月{group}每个品牌的销量占比的同比")
                # qty_ratio_ly.join(qty_ratio, [group, "brand"]) \
                #     .withColumn("qty_ratio_yoy", period_udf(col("qty_ratio"), col("qty_ratio_ly")))

                all_df = qty_ratio.join(qty_amt, [group, "brand_name"], "outer") \
                                .join(qty_amt_last, [group, "brand_name"], "outer") \
                                .join(qty_ratio_last, [group, "brand_name"], "outer") \
                                .join(qty_amt_ly, [group, "brand_name"], "outer") \
                                .join(qty_ratio_ly, [group, "brand_name"], "outer")\
                                .na.fill(0,c)
                columns = cols_comm[i] + c
                all_df.join(brand, "brand_name") \
                    .withColumn("row", f.concat_ws("_", col(group), col("brand_id"))) \
                    .join(join, group) \
                    .withColumn("ciga_data_marker", f.lit(marker)) \
                    .foreachPartition(lambda x: write_hbase1(x, columns, hbase))
            except Exception:
                tb.print_exc()
        co_co_line.unpersist()
    except Exception:
        tb.print_exc()

def get_brand_stats_info_weekly():
    # 市/区每款卷烟上一周销量/环比/同比
    try:
        # 烟品牌id，烟品牌名称
        brand = get_phoenix_table(spark, brand_table)
        # 烟id，烟名称
        plm_item = get_plm_item(spark).select("item_id", "item_name")

        area = get_area(spark)
        # com_id与city的映射关系
        city = area.dropDuplicates(["com_id"]).select("com_id", "city")
        # sale_center_id与区(list)的映射关系
        county = area.groupBy("sale_center_id") \
            .agg(f.collect_list("county").alias("county")) \
            .select("sale_center_id", "county")

        #标识列的值
        markers = ["0", "2"]
        # 按照 市或区统计
        groups = ["com_id", "sale_center_id"]
        joins = [city, county]
        # 除需要计算的值，其他的数据
        cols_comm = [
            ["city", "brand_id", "brand_name", "ciga_data_marker"],
            [ "county", "sale_center_id", "brand_id", "brand_name", "ciga_data_marker"]
        ]
        # 需要计算的值的列名
        cols = [
            ["brand_city_week_sales","brand_city_week2_sales","brand_city_week_sales_last_year"],
            ["brand_county_week_sales","brand_county_week2_sales","brand_county_week_sales_last_year"]
        ]

        co_co_line = get_co_co_line(spark, scope=[1, 2], filter="week") \
            .join(plm_item, "item_id") \
            .withColumn("brand_name", item_name_udf(col("item_name"))) \
            .select("brand_name", "com_id", "sale_center_id", "qty_ord", "week_diff")

        co_co_line.cache()

        last_year = spark.sql("select  * from DB2_DB2INST1_CO_CO_LINE") \
            .where(col("com_id").isin(cities)) \
            .withColumn("born_date", f.to_date("born_date", "yyyyMMdd")) \
            .withColumn("last_year_today", f.date_sub(f.current_date(), 365)) \
            .withColumn("week_diff", week_diff("last_year_today", "born_date")) \
            .where(col("week_diff") == 1) \
            .withColumn("qty_ord", col("qty_ord").cast("float")) \
            .join(plm_item, "item_id") \
            .withColumn("brand_name", item_name_udf(col("item_name"))) \
            .select("brand_name", "com_id","sale_center_id", "qty_ord")
        last_year.cache()
        for i in range(len(groups)):
            group = groups[i]
            join = joins[i]
            c = cols[i]
            marker = markers[i]
            try:
                # 获取上一周订单行表数据
                last_week=co_co_line.where(col("week_diff")==1)

                # 1.市/区每款烟的订单量
                print(f"{str(dt.now())}  上一周{group}每个品牌的销量")
                qty_amt = last_week.groupBy(group, "brand_name").agg(f.sum(col("qty_ord")).alias(c[0]))

                # -------------------环比
                # 获取上第二周订单行表数据
                last_two_week = co_co_line.where(col("week_diff") == 2)

                # 2.市/区每款卷烟销量 上次同期
                print(f"{str(dt.now())}  上第二周{group}每个品牌的销量")
                qty_amt_last = last_two_week.groupBy(group, "brand_name").agg(f.sum(col("qty_ord")).alias(c[1]))

                # # 2.市/区每款卷烟销量 环比
                # print(f"{str(dt.now())}  上一周{group}每个品牌的销量环比")
                # qty_amt_last.join(qty_amt, [group, "brand"]) \
                #     .withColumn("qty_ring_rate", period_udf(col("qty_ord"), col("qty_ord_last")))

                # --------------------同比

                # 3.市/区每款烟的订单量 去年
                print(f"{str(dt.now())}  去年同期{group}每个品牌的销量")
                qty_ord_ly = last_year.groupBy(group, "brand_name") \
                    .agg(f.sum(col("qty_ord")).alias(c[2]))
                # # 3.市/区每款卷烟销量同比
                # print(f"{str(dt.now())}  上一周{group}每个品牌的销量的同比")
                # qty_ord_ly.join(qty_amt, [group, "brand"]) \
                #     .withColumn("qty_ord_yoy", period_udf(col("qty_ord"), col("qty_ord_ly")))

                all_df=qty_amt.join(qty_amt_last,[group,"brand_name"],"outer")\
                       .join(qty_ord_ly,[group,"brand_name"],"outer") \
                       .na.fill(0, c)

                columns = cols_comm[i] + c
                all_df.join(brand, "brand_name") \
                    .withColumn("row", f.concat_ws("_", col(group), col("brand_id"))) \
                    .join(join, group) \
                    .withColumn("ciga_data_marker", f.lit(marker)) \
                    .foreachPartition(lambda x: write_hbase1(x, columns, hbase))
            except Exception:
                tb.print_exc()
        co_co_line.unpersist()
        last_year.unpersist()
    except Exception:
        tb.print_exc()



def get_item_ratio():
    # 市/区 同品牌各规格销量占比
    try:
        # 烟品牌id，烟品牌名称
        brand = get_phoenix_table(spark, brand_table)
        # 烟id，烟名称
        plm_item = get_plm_item(spark).select("item_id", "item_name")

        area = get_area(spark)
        # com_id与city的映射关系
        city = area.dropDuplicates(["com_id"]).select("com_id", "city")
        # sale_center_id与区(list)的映射关系
        county = area.groupBy("sale_center_id") \
            .agg(f.collect_list("county").alias("county")) \
            .select("sale_center_id", "county")

        #标识列的值
        markers = ["1", "3"]
        # 按照 市或区统计
        groups = ["com_id", "sale_center_id"]
        joins = [city, county]
        # 除需要计算的值，其他的数据
        cols_comm = [
            ["city", "brand_id", "brand_name","gauge_id","gauge_name", "ciga_data_marker"],
            ["county", "sale_center_id", "brand_id", "brand_name","gauge_id","gauge_name", "ciga_data_marker"]
        ]
        # 需要计算的值的列名
        cols = [
           "brand_city_gauge_sales_ratio",
            "brand_county_gauge_sales_ratio"
        ]

        # 1.上个月同品牌各规格销量占比
        co_co_line = get_co_co_line(spark, scope=[1, 1], filter="month") \
                    .join(plm_item, "item_id") \
                    .withColumn("brand_name", item_name_udf(col("item_name"))) \
                    .select("item_id", "brand_name", "qty_ord", "com_id","sale_center_id")
        co_co_line.cache()
        for i in range(len(groups)):
            group = groups[i]
            join = joins[i]
            c = cols[i]
            marker = markers[i]
            columns=cols_comm[i]
            try:

                #2.各市/区 各品牌销量
                brand_ord = co_co_line.groupBy(group,"brand_name").agg(f.sum(col("qty_ord")).alias("brand_ord"))
                #3.各市/区 每个品牌各品规销量
                item_ord = co_co_line.groupBy(group,"brand_name", "item_id").agg(f.sum(col("qty_ord")).alias("item_ord"))
                #4.销量占比
                print(f"{str(dt.now())}  上一个月{group}同品牌各品规的销量占比")
                brand_item=brand_ord.join(item_ord, [group,"brand_name"]) \
                    .withColumn(c, col("item_ord") / col("brand_ord"))

                columns.append(c)
                brand_item.withColumn("row",f.concat_ws("_",col(group),col("item_id"))) \
                          .join(plm_item,"item_id")\
                          .join(brand, "brand_name")\
                          .join(join,group)\
                          .withColumn("ciga_data_marker",f.lit(marker))\
                          .withColumnRenamed("item_id","gauge_id")\
                          .withColumnRenamed("item_name","gauge_name")\
                          .foreachPartition(lambda x:write_hbase1(x,columns,hbase))
            except Exception:
                tb.print_exc()

        co_co_line.unpersist()
    except Exception:
        tb.print_exc()






#-----------------------品规统计指标


def get_item_stats_info_daily():
    #本月当前本市/区每款卷烟订货量
    #本月当前本市/区每款卷烟订单额

    #烟id，烟名称
    plm_item=get_plm_item(spark).select("item_id","item_name")

    area = get_area(spark)
    #com_id与city的映射关系
    city = area.dropDuplicates(["com_id"]).select("com_id","city")
    #sale_center_id与区(list)的映射关系
    county = area.groupBy("sale_center_id")\
                        .agg(f.collect_list("county").alias("county"))\
                        .select("sale_center_id","county")
    #标识列的值
    markers=["1","3"]
    #按照 市或区统计
    groups = ["com_id", "sale_center_id"]
    joins=[city,county]
    # 除需要计算的值，其他的数据
    cols_comm=[["city","gauge_id","gauge_name","ciga_data_marker"],
             ["county","sale_center_id","gauge_id","gauge_name","ciga_data_marker"]]
    #需要计算的值的列名
    cols=[["gauge_city_orders","gauge_city_order_amount"],["gauge_county_orders","gauge_county_order_amount"]]

    co_co_line = get_co_co_line(spark, scope=[0, 0], filter="month")\
                                 .select("item_id","qty_ord","amt","com_id","sale_center_id")
    co_co_line.cache()
    for i in range(len(groups)):
        group=groups[i]
        join=joins[i]
        c=cols[i]
        marker=markers[i]
        try:

            #2.本月每款烟在每个区的订单量,订单额
            print(f"{str(dt.now())}  本月{group}各品规的订单量,订单额")
            #com_id item_id qty_ord amt
            qty_amt=co_co_line.groupBy([group,"item_id"])\
                          .agg(f.sum(col("qty_ord")).alias(c[0]),f.sum(col("amt")).alias(c[1]))

            columns=cols_comm[i]+c
            qty_amt.withColumn("row",f.concat_ws("_",col(group),col("item_id")))\
                    .join(plm_item,"item_id")\
                    .join(join,group)\
                    .withColumnRenamed("item_id","gauge_id")\
                    .withColumnRenamed("item_name","gauge_name")\
                    .withColumn("ciga_data_marker",f.lit(marker))\
                    .foreachPartition(lambda x:write_hbase1(x,columns,hbase))
        except Exception:
            tb.print_exc()
    co_co_line.unpersist()




def get_item_stats_info_monthly():
    # 市/区每款卷烟上一月销量/环比/同比
    # 市/区每款卷烟上一月订单数/环比/同比
    # 市/区每款卷烟上一月销量占比/环比/同比

    try:
        # 烟id，烟名称
        plm_item = get_plm_item(spark).select("item_id", "item_name")

        area = get_area(spark)
        # com_id与city的映射关系
        city = area.dropDuplicates(["com_id"]).select("com_id", "city")
        # sale_center_id与区(list)的映射关系
        county = area.groupBy("sale_center_id") \
            .agg(f.collect_list("county").alias("county")) \
            .select("sale_center_id", "county")

        # 标识列的值
        markers = ["1", "3"]
        # 按照 市或区统计
        groups = ["com_id", "sale_center_id"]
        joins = [city, county]
        # 除需要计算的值，其他的数据
        cols_comm = [
                     [ "city", "gauge_id", "gauge_name", "ciga_data_marker"],
                     [ "county", "sale_center_id", "gauge_id", "gauge_name", "ciga_data_marker"]
                    ]
        # 需要计算的值的列名
        cols = [
                ["gauge_city_month_sales","gauge_city_month_orders","gauge_city_month_sales_ratio",
                 "gauge_city_month2_sales","gauge_city_month2_orders","gauge_city_month2_sales_ratio",
                 "gauge_city_month_sales_last_year","gauge_city_month_orders_last_year","gauge_city_month_retio_last_year"
                 ],
                ["gauge_county_month_sales","gauge_county_month_orders","gauge_county_month_sales_ratio",
                 "gauge_county_month2_sales","gauge_county_month2_orders","gauge_county_month2_sales_ratio",
                 "gauge_county_month_sales_last_year","gauge_county_month_orders_last_year","gauge_county_month_retio_last_year"
                ]
              ]

        co_co_line = get_co_co_line(spark, scope=[1, 13], filter="month") \
            .where(col("month_diff").isin([1,2,13]))\
            .select("item_id",  "com_id", "sale_center_id", "qty_ord","month_diff")

        co_co_line.cache()

        for i in range(len(groups)):
            group = groups[i]
            join = joins[i]
            c = cols[i]
            marker=markers[i]
            try:
                # 获取上一个月订单行表数据
                last_month = co_co_line.where(col("month_diff")==1)

                # 1.1、1.2.市/区每款烟的订单量，订单数
                print(f"{str(dt.now())}  上一个月{group}每款烟的订单量，订单数")
                qty_amt = last_month.groupBy(group, "item_id")\
                                     .agg(f.sum(col("qty_ord")).alias(c[0]),f.count(col(group)).alias(c[1]))

                # 1.3.市/区每款卷烟销量占比
                print(f"{str(dt.now())}  上一个月{group}每款烟的销量占比")
                qty_ratio = last_month.groupBy(group) \
                    .agg(f.sum(col("qty_ord")).alias("qty_ord_total")) \
                    .join(qty_amt, group) \
                    .withColumn(c[2], col(c[0]) / col("qty_ord_total"))\
                    .select(group,"item_id",c[2])

                # -----------------------------------------------------环比
                # 先计算上第二个月 的订单量，订单数 ，销量占比，再join 上个月的
                # 获取上第二个月订单行表数据
                last_two_month = co_co_line.where(col("month_diff") == 2)

                # 2.1、2.2市/区每款卷烟订单量，订单数 上次同期
                print(f"{str(dt.now())}  上第二个月{group}每款烟的订单量，订单数")
                qty_amt_last = last_two_month.groupBy(group, "item_id")\
                                         .agg(f.sum(col("qty_ord")).alias(c[3]),f.count(col(group)).alias(c[4]))
                # 2.3市/区每款卷烟销量占比 上次同期
                print(f"{str(dt.now())}  上第二个月{group}每款卷烟销量占比")
                qty_ratio_last = last_two_month.groupBy(group) \
                    .agg(f.sum(col("qty_ord")).alias("qty_ord_total_last")) \
                    .join(qty_amt_last, group) \
                    .withColumn(c[5], col(c[3]) / col("qty_ord_total_last"))\
                    .select(group,"item_id",c[5])

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
                last_year = co_co_line.where(col("month_diff") == 13)

                # 3.1、3.2市/区每款烟的订单量,订单数 去年同期
                print(f"{str(dt.now())}  去年同期{group}每款卷烟销量占比")
                qty_amt_ly = last_year.groupBy(group, "item_id") \
                                     .agg(f.sum(col("qty_ord")).alias(c[6]), f.count(col(group)).alias(c[7]))

                # 3.3市/区每款烟的占比 去年同期
                print(f"{str(dt.now())}  去年同期{group}每款烟的订单量，订单数")
                qty_ratio_ly = last_year.groupBy(group) \
                    .agg(f.sum(col("qty_ord")).alias("qty_ord_total")) \
                    .join(qty_amt_ly, group) \
                    .withColumn(c[8], col(c[6]) / col("qty_ord_total"))\
                     .select(group,"item_id",c[8])

                # # 7、8. 市/区每款卷烟订单量,订单数 同比
                # print(f"{str(dt.now())}  上一个月{group}每款烟的订单量，订单数同比")
                # qty_amt_ly.join(qty_amt, [group, "item_id"]) \
                #     .withColumn("qty_ord_yoy", period_udf(col("qty_ord"), col("qty_ord_ly"))) \
                #     .withColumn("amt_yoy", period_udf(col("amount"), col("amount_ly")))
                # # 9.市/区每款烟销量占比的同比
                # print(f"{str(dt.now())}  上一个月{group}每款烟的销量占比的同比")
                # qty_ratio_ly.join(qty_ratio, [group, "item_id"]) \
                #     .withColumn("qty_ratio_yoy", period_udf(col("qty_ratio"), col("qty_ratio_ly")))

                all_df=qty_amt.join(qty_ratio,[group,"item_id"],"outer")\
                       .join(qty_amt_last,[group,"item_id"],"outer")\
                       .join(qty_ratio_last,[group,"item_id"],"outer")\
                       .join(qty_amt_ly,[group,"item_id"],"outer")\
                       .join(qty_ratio_ly,[group,"item_id"],"outer")\
                       .na.fill(0,c)
                columns=cols_comm[i]+c
                all_df.withColumn("row",f.concat_ws("_",col(group),col("item_id")))\
                      .join(plm_item,"item_id")\
                      .join(join,group)\
                      .withColumnRenamed("item_id","gauge_id")\
                      .withColumnRenamed("item_name","gauge_name")\
                      .withColumn("ciga_data_marker",f.lit(marker))\
                      .foreachPartition(lambda x:write_hbase1(x,columns,hbase))
            except Exception:
                tb.print_exc()
        co_co_line.unpersist()
    except Exception:
        tb.print_exc()




def get_item_stats_info_weekly():
    # 市/区每款卷烟上一周销量/环比/同比
    try:
        # 烟id，烟名称
        plm_item = get_plm_item(spark).select("item_id", "item_name")

        area = get_area(spark)
        # com_id与city的映射关系
        city = area.dropDuplicates(["com_id"]).select("com_id", "city")
        # sale_center_id与区(list)的映射关系
        county = area.groupBy("sale_center_id") \
            .agg(f.collect_list("county").alias("county")) \
            .select("sale_center_id", "county")

        # 标识列的值
        markers = ["1", "3"]
        # 按照 市或区统计
        groups = ["com_id", "sale_center_id"]
        joins = [city, county]
        # 除需要计算的值，其他的数据
        cols_comm = [
            [ "city", "gauge_id", "gauge_name", "ciga_data_marker"],
            [ "county", "sale_center_id", "gauge_id", "gauge_name", "ciga_data_marker"]
        ]
        # 需要计算的值的列名
        cols = [
            ["gauge_city_week_sales", "gauge_city_week_sales_last_year", "gauge_city_week2_sales"],
            ["gauge_county_week_sales", "gauge_county_week_sales_last_year", "gauge_county_week2_sales"]
        ]

        co_co_line = get_co_co_line(spark, scope=[1, 2], filter="week") \
            .select("item_id", "com_id", "sale_center_id", "qty_ord", "week_diff")

        co_co_line.cache()

        last_year = spark.sql("select  * from DB2_DB2INST1_CO_CO_LINE") \
            .where(col("com_id").isin(cities)) \
            .withColumn("born_date", f.to_date("born_date", "yyyyMMdd")) \
            .withColumn("last_year_today", f.date_sub(f.current_date(), 365)) \
            .withColumn("week_diff", week_diff("last_year_today", "born_date")) \
            .where(col("week_diff") == 1) \
            .withColumn("qty_ord", col("qty_ord").cast("float")) \
            .select("com_id","sale_center_id", "item_id", "qty_ord")
        last_year.cache()
        for i in range(len(groups)):
            group = groups[i]
            join = joins[i]
            c = cols[i]
            marker = markers[i]
            try:
                # 获取上一周订单行表数据
                last_week=co_co_line.where(col("week_diff")==1)

                # 1.市/区每款烟的订单量
                print(f"{str(dt.now())}  上一周{group}每款烟的订单量")
                qty_amt = last_week.groupBy(group, "item_id").agg(f.sum(col("qty_ord")).alias(c[0]))

                # -------------------环比
                # 获取上第二周订单行表数据
                last_two_week = co_co_line.where(col("week_diff") == 2)

                # 2.市/区每款卷烟销量 上次同期
                print(f"{str(dt.now())}  上第二周{group}每款烟的订单量")
                qty_amt_last = last_two_week.groupBy(group, "item_id").agg(f.sum(col("qty_ord")).alias(c[1]))

                # # 2.市/区每款卷烟销量 环比
                # print(f"{str(dt.now())}  上一周{group}每款烟销量环比")
                # qty_amt_last.join(qty_amt, [group, "item_id"]) \
                #     .withColumn("qty_ring_rate", period_udf(col("qty_ord"), col("qty_ord_last")))

                # --------------------同比

                # 3.市/区每款烟的订单量 去年
                print(f"{str(dt.now())}  去年同期{group}每款烟的订单量")
                qty_ord_ly = last_year.groupBy(group, "item_id") \
                    .agg(f.sum(col("qty_ord")).alias(c[2]))

                # #3 市/区每款卷烟销量同比
                # print(f"{str(dt.now())}  上一周{group}每款烟销量同比")
                # qty_ord_ly.join(qty_amt, [group, "item_id"]) \
                #     .withColumn("qty_ord_yoy", period_udf(col("qty_ord"), col("qty_ord_ly")))

                all_df = qty_amt.join(qty_amt_last, [group, "item_id"], "outer") \
                    .join(qty_ord_ly, [group, "item_id"], "outer")\
                    .na.fill(0,c)

                columns = cols_comm[i] + c
                all_df.withColumn("row", f.concat_ws("_", col(group), col("item_id"))) \
                    .join(plm_item, "item_id") \
                    .join(join, group) \
                    .withColumnRenamed("item_id", "gauge_id") \
                    .withColumnRenamed("item_name", "gauge_name") \
                    .withColumn("ciga_data_marker", f.lit(marker)) \
                    .foreachPartition(lambda x: write_hbase1(x, columns, hbase))
            except Exception:
                tb.print_exc()
        co_co_line.unpersist()
        last_year.unpersist()
    except Exception:
        tb.print_exc()




def get_item_historical_sales():
    #市/区每款卷烟上四周各周的销量
    try:

        # 烟id，烟名称
        plm_item = get_plm_item(spark).select("item_id", "item_name")

        area = get_area(spark)
        # com_id与city的映射关系
        city = area.dropDuplicates(["com_id"]).select("com_id", "city")
        # sale_center_id与区(list)的映射关系
        county = area.groupBy("sale_center_id") \
            .agg(f.collect_list("county").alias("county")) \
            .select("sale_center_id", "county")

        # 标识列的值
        markers = ["1", "3"]
        # 按照 市或区统计
        groups = ["com_id", "sale_center_id"]
        joins = [city, county]
        # 除需要计算的值，其他的数据
        cols_comm = [
            ["city", "gauge_id", "gauge_name", "ciga_data_marker"],
            ["county", "sale_center_id", "gauge_id", "gauge_name", "ciga_data_marker"]
        ]
        # 需要计算的值的列名
        cols = ["gauge_city_sales_history","gauge_county_sales_history"]


        # 获取上四周订单行表数据
        # date为订单所在周的星期五的日期 给前端展示
        co_co_line = get_co_co_line(spark, scope=[1, 4], filter="week") \
            .select("item_id", "com_id","sale_center_id", "qty_ord", "born_date") \
            .withColumn("date", f.date_add(f.date_trunc("week", col("born_date")), 4))
        co_co_line.cache()
        for i in range(len(groups)):
            group = groups[i]
            join = joins[i]
            c = cols[i]
            marker = markers[i]
            columns=cols_comm[i]
            print(f"{str(dt.now())} {group} 每款卷烟上四周各周的销量")
            try:
                json_udf=f.udf(lambda x,y:json.dumps({"date":str(x),"value":y}))
                #计算每个市/区 每款烟前四周各周的销量
                #将结果拼成 [{"date":"2019-06-21","value":12354},{"date":"2019-06-14","value":14331}....]
                result=co_co_line.groupBy(group,"item_id","date")\
                          .agg(f.sum(col("qty_ord")).alias("qty_ord"))\
                          .withColumn("json",json_udf(col("date"),col("qty_ord")))\
                          .groupBy(group,"item_id")\
                          .agg(f.collect_list(col("json")).alias(c))


                columns.append(c)
                result.withColumn("row", f.concat_ws("_", col(group), col("item_id"))) \
                    .join(plm_item, "item_id") \
                    .join(join, group) \
                    .withColumnRenamed("item_id", "gauge_id") \
                    .withColumnRenamed("item_name", "gauge_name") \
                    .withColumn("ciga_data_marker", f.lit(marker)) \
                    .foreachPartition(lambda x: write_hbase1(x, columns, hbase))
            except Exception:
                tb.print_exc()
        co_co_line.unpersist()
    except Exception:
        tb.print_exc()






def get_item_sales_forecast():
    #周销量未来预测

    try:
        print(f"{str(dt.now())} 周销量未来预测")
        spark.conf.set("spark.sql.execution.arrow.enabled","true")
        co_co_line=get_co_co_line(spark,scope=[1,365]).select("co_num","line_num","cust_id","item_id","sale_center_id",
                                                   "qty_need","qty_ord","qty_rsn","price","amt","born_date")\
                                                  .withColumn("born_date",col("born_date").cast("string"))

        pd_df=co_co_line.toPandas()
        #获取周销量未来两周预测结果
        """
        item_id,sale_center_id,2019-06-03 00:00:00,2019-06-10 00:00:00
        1130309,01111430204,8,10
        1130309,01111430205,45,51
        """
        sub_df = predict(pd_df, 2)

        #转成spark的DataFrame
        result=spark.createDataFrame(sub_df)
        #将日期作为列值    item_id,sale_center_id,week1,value1,week2,value2
        columns=result.columns[2:4]
        for i in range(len(columns)):
            column=columns[i]
            result=result.withColumnRenamed(column,f"value{i+1}")\
                          .withColumn(f"week{i+1}",f.lit(column))

        json_udf=f.udf(lambda x1,y1,x2,y2:json.dumps([{"date":x1,"value":y1},{"date":x2,"value":y2}]))
        colName="gauge_sales_forecast"
        result=result.withColumn(colName,json_udf(col("week1"),col("value1"),col("week2"),col("value2")))



        plm_item = get_plm_item(spark).select("item_id", "item_name")

        area = get_area(spark)
        # sale_center_id与区(list)的映射关系
        county = area.groupBy("sale_center_id") \
            .agg(f.collect_list("county").alias("county")) \
            .select("sale_center_id", "county")

        columns=["sale_center_id","county","gauge_id","gauge_name","ciga_data_marker",colName]
        result.join(plm_item,"item_id")\
              .join(county,"sale_center_id")\
              .withColumn("row",f.concat_ws("_",col("sale_center_id"),col("item_id")))\
              .withColumn("ciga_data_marker",f.lit("3"))\
              .withColumnRenamed("item_id","gauge_id")\
              .withColumnRenamed("item_name","gauge_name")\
              .foreachPartition(lambda x:write_hbase1(x,columns,hbase))

    except Exception:
        tb.print_exc()





def get_cover_rate():
    #上个月店铺覆盖率

    try:
        # 烟id，烟名称
        plm_item = get_plm_item(spark).select("item_id", "item_name")

        area = get_area(spark)
        # com_id与city的映射关系
        city = area.dropDuplicates(["com_id"]).select("com_id", "city")
        # sale_center_id与区(list)的映射关系
        county = area.groupBy("sale_center_id") \
            .agg(f.collect_list("county").alias("county")) \
            .select("sale_center_id", "county")

        # 标识列的值
        markers = ["1", "3"]
        # 按照 市或区统计
        groups = ["com_id", "sale_center_id"]
        joins = [city, county]
        # 除需要计算的值，其他的数据
        cols_comm = [
            ["city",  "gauge_id", "gauge_name", "ciga_data_marker"],
            ["county", "sale_center_id","gauge_id", "gauge_name", "ciga_data_marker"]
        ]
        # 需要计算的值的列名
        cols = [
            "gauge_city_retail_ratio",
            "gauge_county_retail_ratio"
        ]

        co_co_line = get_co_co_line(spark, scope=[1, 1], filter="month") \
            .select("item_id", "cust_id", "com_id","sale_center_id")
        co_co_line.cache()

        for i in range(len(groups)):
            group = groups[i]
            join = joins[i]
            c = cols[i]
            marker = markers[i]
            columns=cols_comm[i]
            try:
                co_cust = get_co_cust(spark).select("cust_id",group)

                print(f"{str(dt.now())}  卷烟店铺覆盖率  {group}级别")

                # 1.每个区域零售户数量
                cust_num = co_cust.groupBy(group).agg(f.count("cust_id").alias("cust_num"))
                # 2.每个区域每款卷烟覆盖店面数量
                item_cover_num = co_co_line.dropDuplicates(["cust_id", "item_id"]) \
                    .groupBy(group, "item_id") \
                    .agg(f.count("cust_id").alias("item_cover_num"))

                #3.店铺覆盖率
                cover_ratio=item_cover_num.join(cust_num, group) \
                    .withColumn(c, col("item_cover_num") / col("cust_num"))

                columns.append(c)
                cover_ratio.withColumn("row", f.concat_ws("_", col(group), col("item_id"))) \
                    .join(plm_item, "item_id") \
                    .join(join, group) \
                    .withColumn("ciga_data_marker", f.lit(marker)) \
                    .withColumnRenamed("item_id", "gauge_id") \
                    .withColumnRenamed("item_name", "gauge_name") \
                    .foreachPartition(lambda x: write_hbase1(x, columns, hbase))
            except Exception:
                tb.print_exc()
        co_co_line.unpersist()
    except Exception:
        tb.print_exc()





def get_static_similar_ciagr():
    # 静态属性相似卷烟
    try:
        print(f"{str(dt.now())} 静态属性相似卷烟")

        ciga_static = get_phoenix_table(spark, "V630_TOBACCO.CIGA_STATIC")
        rename = {
            "gauge_id": "item_id", "ciga_price": "price", "gauge_name": "item_name",
            "ciga_class": "kind", "ciga_type": "product_type", "ciga_origin_type": "yieldly_type",
            "is_thin": "is_thin", "tar_cont": "tar_cont", "gas_nicotine": "gas_nicotine",
            "co_cont": "co_cont", "ciga_long": "cigar_long", "m_long": "m_long",
            "package_type": "package_type", "box_num": "box_num", "sales_type": "sales_type"
        }
        columns = list(rename.keys())
        for column in columns:
            ciga_static = ciga_static.withColumnRenamed(column, rename[column])
        df = ciga_static.select(list(rename.values()))

        pd_df = df.toPandas()

        similar_cigar = spark.createDataFrame(get_similar_cigar(pd_df))
        plm_item = get_plm_item(spark).select("item_id", "item_name")



        colName="gauge_prop_like_ciga"

        columns = ["gauge_id", "gauge_name", colName, "ciga_data_marker"]
        json_udf = f.udf(
            lambda x, y, z: json.dumps({"gaugeId": x, "gaugeName": y, "gaugeSimilarity": z}, ensure_ascii=False))
        # similar_cigar.join(plm_item, "item_id") \
        #     .withColumnRenamed("item_id", "gauge_id") \
        #     .withColumnRenamed("item_name", "gauge_name") \
        #     .join(plm_item, col("s_item_id") == col("item_id")) \
        #     .withColumn(colName, json_udf(col("item_id"), col("item_name"), col("similarity"))) \
        #     .withColumn("row", col("gauge_id")) \
        #     .withColumn("ciga_data_marker", f.lit("4")) \
        #     .foreachPartition(lambda x: write_hbase1(x, columns, hbase))

        #从大到小排序
        sorted_udf = f.udf(lambda l: sorted(l, key=lambda x: json.loads(x)["gaugeSimilarity"], reverse=True))
        similar_cigar.join(plm_item, "item_id") \
            .withColumnRenamed("item_id", "gauge_id") \
            .withColumnRenamed("item_name", "gauge_name") \
            .join(plm_item, col("s_item_id") == col("item_id")) \
            .withColumn(colName, json_udf(col("item_id"), col("item_name"), col("similarity"))) \
            .groupBy("gauge_id","gauge_name") \
            .agg(f.collect_list(col(colName)).alias(colName)) \
            .withColumn(colName, sorted_udf(col(colName))) \
            .withColumn("row", col("gauge_id")) \
            .withColumn("ciga_data_marker", f.lit("4")) \
            .foreachPartition(lambda x: write_hbase1(x, columns, hbase))
    except Exception:
        tb.print_exc()





def get_order_similar_cigar():
    #客户订购相似卷烟
    try:
        print(f"{str(dt.now())} 客户订购相似卷烟")
        # origin为要查找的烟，nearest为最相似的卷烟
        similar_cigar=get_client_similar_cigar(spark)

        plm_item = get_plm_item(spark).select("item_id", "item_name")

        columns = ["gauge_id", "gauge_name", "gauge_client_like_ciga", "ciga_data_marker"]
        json_udf = f.udf(
            lambda x, y, z: json.dumps({"gaugeId": x, "gaugeName": y, "gaugeSimilarity": z}, ensure_ascii=False))

        df_map = similar_cigar.join(plm_item, col("origin") == col("item_id")) \
            .withColumnRenamed("item_id", "gauge_id") \
            .withColumnRenamed("item_name", "gauge_name") \
            .join(plm_item, col("other") == col("item_id")) \
            .withColumn("item_map", json_udf(col("item_id"), col("item_name"), col("distance")))

        #按照相似度倒序排序
        map_list_sort_udf = f.udf(lambda l, k: sorted(l, key=lambda x: json.loads(x)[k], reverse=True))

        df_map.groupBy("gauge_id", "gauge_name") \
            .agg(f.collect_list(col("item_map")).alias("item_list")) \
            .withColumn("gauge_client_like_ciga", map_list_sort_udf(col("item_list"), f.lit("gaugeSimilarity"))) \
            .withColumn("row", col("gauge_id")) \
            .withColumn("ciga_data_marker", f.lit("4")) \
            .select("row", "gauge_id", "gauge_name", "ciga_data_marker", "gauge_client_like_ciga") \
            .foreachPartition(lambda x: write_hbase1(x, columns, hbase))
    except Exception:
        tb.print_exc()





def get_brand_rating():
    # 品牌卷烟区域偏好分布
    hbase = {"table": als_table+"_TEMP", "families": ["0"], "row": "row"}
    try:
        #卷烟品规id 卷烟品规名称
        brand=get_phoenix_table(spark, brand_table).select("brand_id","brand_name")

        area = get_area(spark)
        # com_id与city的映射关系
        city = area.dropDuplicates(["com_id"]).select("com_id", "city")
        # sale_center_id与区(list)的映射关系
        county = area.groupBy("sale_center_id") \
                .agg(f.collect_list("county").alias("county")) \
                .select("sale_center_id", "county")

        #com_id,sale_center_id,city,county,cust_id,cust_name,longitude,latitude
        co_cust = get_co_cust(spark).select("cust_id", "cust_name", "com_id", "sale_center_id") \
                                    .join(get_cust_lng_lat(spark), "cust_id") \
                                    .withColumnRenamed("lng", "longitude") \
                                    .withColumnRenamed("lat", "latitude")\
                                    .join(county,"sale_center_id")\
                                     .join(city,"com_id")

        columns = ["city","sale_center_id","county","cust_id","cust_name",
                       "longitude","latitude","brand_id", "brand_name", "grade_data_marker","brand_grade"]


        print(f"{str(dt.now())} 每个零售户对每品牌烟的评分")

        get_rating(spark, "brand_name")\
               .join(co_cust,"cust_id")\
               .join(brand,"brand_name")\
               .withColumn("row",f.concat_ws("_",col("brand_id"),col("cust_id")))\
               .withColumn("grade_data_marker",f.lit("0"))\
               .withColumnRenamed("rating","brand_grade")\
               .foreachPartition(lambda x:write_hbase1(x,columns,hbase))
    except Exception:
        tb.print_exc()





def get_item_rating():
    #规格卷烟区域偏好分布
    hbase = {"table": als_table+"_TEMP", "families": ["0"], "row": "row"}
    try:
        #卷烟id 卷烟名称
        plm_item = get_plm_item(spark).select("item_id", "item_name")

        area = get_area(spark)
        # com_id与city的映射关系
        city = area.dropDuplicates(["com_id"]).select("com_id", "city")
        # sale_center_id与区(list)的映射关系
        county = area.groupBy("sale_center_id") \
                .agg(f.collect_list("county").alias("county")) \
                .select("sale_center_id", "county")

        #com_id,sale_center_id,city,county,cust_id,cust_name,longitude,latitude
        co_cust = get_co_cust(spark).select("cust_id", "cust_name", "com_id", "sale_center_id") \
                                    .join(get_cust_lng_lat(spark), "cust_id") \
                                    .withColumnRenamed("lng", "longitude") \
                                    .withColumnRenamed("lat", "latitude")\
                                    .join(county,"sale_center_id")\
                                    .join(city,"com_id")

        print(f"{str(dt.now())} 每个零售户对每品规烟的评分")
        columns=["city","sale_center_id", "county", "cust_id", "cust_name",
                       "longitude", "latitude", "gauge_id", "gauge_name","grade_data_marker","gauge_grade"]

        get_cigar_rating(spark).join(co_cust,"cust_id")\
                                  .join(plm_item,"item_id")\
                                  .withColumn("row",f.concat_ws("_",col("item_id"),col("cust_id")))\
                                  .withColumn("grade_data_marker",f.lit("1"))\
                                  .withColumnRenamed("item_id","gauge_id")\
                                  .withColumnRenamed("item_name","gauge_name")\
                                  .withColumnRenamed("rating","gauge_grade")\
                                  .foreachPartition(lambda x:write_hbase1(x,columns,hbase))

    except Exception:
        tb.print_exc()