#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/6/20 14:28
import traceback as tb
from pyspark.sql.functions import col
from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from pyspark import SparkContext
from datetime import datetime as dt
from pyspark.sql import Window
import json
from rules.write_hbase import write_hbase1
from rules.config import goods_supply_table
from rules.utils import *
from ml.forecast_qtyrsn.predict_sales import predict

spark = SparkSession.builder.enableHiveSupport().appName("goods supply").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

spark.sql("use aistrong")

hbase = {"table": goods_supply_table+"_TEMP", "families": ["0"], "row": "row"}


def city_reality_supply():
    # 全市上周该品规计划投放量
    # 全市上周该品规实际投放量
    # 全市上周该品规投放剩余量
    try:
        print(f"{str(dt.now())} 全市上周各品规 计划/实际/剩余 量")
        plm_item = get_plm_item(spark).select("item_id", "item_name")

        area = get_area(spark)
        # com_id与city的映射关系
        city = area.dropDuplicates(["com_id"]).select("com_id", "city")

        # 获取上周
        # cust_item_spw = spark.sql(
        #     "select com_id,item_id,qty_plan,qty_remain,qty_allocco,begin_date,end_date from DB2_DB2INST1_SGP_CUST_ITEM_SPW") \
        #     .where(col("com_id").isin(cities)) \
        #     .withColumn("begin_date", f.to_date(col("begin_date"), "yyyyMMdd")) \
        #     .withColumn("end_date", f.to_date(col("end_date"), "yyyyMMdd")) \
        #     .withColumn("last_mon", f.date_sub(f.date_trunc("week", f.current_date()), 7)) \
        #     .withColumn("last_sun", f.date_add(col("last_mon"), 6)) \
        #     .where((col("begin_date") == col("last_mon")) & (col("end_date") == col("last_sun")))

        cust_item_spw = spark.sql(
            "select com_id,item_id,qty_plan,qty_remain,qty_allocco,begin_date,end_date from DB2_DB2INST1_SGP_CUST_ITEM_SPW") \
            .where(col("com_id").isin(cities)) \
            .where((col("begin_date") == "20190708") & (col("end_date") <= "20190714"))


        #需要计算的值的列名
        cols = ["gauge_week_planned_volume", "gauge_week_reality_volume", "gauge_week_residue_volume"]
        result = cust_item_spw.groupBy("com_id", "item_id") \
            .agg(f.sum("qty_plan").alias(cols[0]),
                 f.sum("qty_allocco").alias(cols[1]),
                 f.sum("qty_remain").alias(cols[2]))

        columns = ["gauge_id", "gauge_name", "city", "gears_data_marker"] + cols
        result.withColumn("row", f.concat_ws("_", col("com_id"), col("item_id"))) \
            .withColumn("gears_data_marker", f.lit("0")) \
            .join(plm_item, "item_id") \
            .join(city, "com_id") \
            .withColumnRenamed("item_id","gauge_id")\
             .withColumnRenamed("item_name","gauge_name")\
            .foreachPartition(lambda x: write_hbase1(x, columns, hbase))
    except Exception:
        tb.print_exc()





def county_reality_supply():
    # 各区县各档位该品规上周投放量
    try:
        print(f"{str(dt.now())} 各区县各档位该品规上周投放量")
        plm_item = get_plm_item(spark).select("item_id", "item_name")

        co_cust=get_co_cust(spark).select("cust_id","sale_center_id","cust_seg")

        area = get_area(spark)
        # com_id与city的映射关系
        city = area.dropDuplicates(["com_id"]).select("com_id", "city")
        # sale_center_id与区(list)的映射关系
        county = area.groupBy("sale_center_id") \
            .agg(f.collect_list("county").alias("county")) \
            .select("sale_center_id", "county")

        # 获取上周实际投放量
        # cust_item_spw = spark.sql(
        #     "select com_id,cust_id,item_id,qty_allocco,begin_date,end_date from DB2_DB2INST1_SGP_CUST_ITEM_SPW") \
        #     .withColumn("begin_date", f.to_date(col("begin_date"), "yyyyMMdd")) \
        #     .withColumn("end_date", f.to_date(col("end_date"), "yyyyMMdd")) \
        #     .withColumn("last_mon", f.date_sub(f.date_trunc("week", f.current_date()), 7)) \
        #     .withColumn("last_sun", f.date_add(col("last_mon"), 6)) \
        #     .where((col("begin_date") == col("last_mon")) & (col("end_date") == col("last_sun")))\
        #     .join(co_cust,"cust_id")

        cust_item_spw = spark.sql(
            "select com_id,cust_id,item_id,qty_allocco,begin_date,end_date from DB2_DB2INST1_SGP_CUST_ITEM_SPW") \
            .withColumn("begin_date", f.to_date(col("begin_date"), "yyyyMMdd")) \
            .withColumn("end_date", f.to_date(col("end_date"), "yyyyMMdd")) \
            .withColumn("last_mon", f.date_sub(f.date_trunc("week", f.current_date()), 7 * 4)) \
            .withColumn("last_sun", f.date_add(col("last_mon"), 6 + 7 * 3)) \
            .where((col("begin_date") >= col("last_mon")) & (col("end_date") <= col("last_sun")))\
            .join(co_cust,"cust_id")



        #需要计算的值的列名
        colName = "county_gauge_week_volume"
        result = cust_item_spw.groupBy("com_id","sale_center_id","cust_seg", "item_id") \
                                .agg(f.sum("qty_allocco").alias(colName))

        columns = ["com_id","city","sale_center_id","county","gears","gauge_id", "gauge_name", "city", "gears_data_marker",colName]
        result.withColumn("row", f.concat_ws("_", col("sale_center_id"),col("cust_seg"), col("item_id"))) \
            .withColumn("gears_data_marker", f.lit("4")) \
            .join(plm_item, "item_id") \
            .join(city, "com_id") \
            .join(county,"sale_center_id")\
            .withColumnRenamed("item_id","gauge_id")\
            .withColumnRenamed("item_name","gauge_name")\
            .withColumnRenamed("cust_seg","gears")\
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
        city = area.dropDuplicates(["com_id"]).select("com_id", "city")

        co_co_line = get_co_co_line(spark, scope=[1, 1], filter="week") \
            .where(col("qty_rsn")>0)\
            .select("com_id", "cust_id", "item_id", "qty_ord", "qty_rsn")

        co_co_line.cache()

        # 只按照档位过滤掉ZZ档位的，状态不过滤
        co_cust = spark.sql("select cust_id from DB2_DB2INST1_CO_CUST where dt=(select max(dt) from DB2_DB2INST1_CO_CUST) and cust_seg!='ZZ'") \
            .where(col("com_id").isin(cities))
        co_cust.cache()

        co_co_line0=co_co_line.join(co_cust,"cust_id")
        try:
            print(f"{str(dt.now())} 全市上周该品规订足率")
            colName = "gauge_week_reserve_ratio"
            reserve_ratio = co_co_line0\
                .groupBy("com_id", "item_id") \
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
            json_udf = f.udf(lambda x, y,z: json.dumps({"gaugeId":x,"gaugeName":y,"gaugeReserve": z},ensure_ascii=False))
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
            total = co_co_line0.groupBy("com_id", "item_id") \
                .agg(f.count(col("item_id")).alias("total"))

            reserve_face = co_co_line0.where(col("qty_ord") == col("qty_rsn")) \
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
            # 重购率 =（本期和上期均订购该品规的客户数÷上期所有订购该品规的客户数）
            # 上上周
            last_two_week = get_co_co_line(spark, scope=[2, 2], filter="week")\
                                   .select("com_id", "cust_id", "item_id","qty_ord")

            # 上上周哪些零售户订了这款烟
            last_two_item = last_two_week.where(col("qty_ord") > 0) \
                .dropDuplicates(["cust_id", "item_id"])
            # 上周哪些零售户订了这款烟
            item = co_co_line.where(col("qty_ord") > 0) \
                .dropDuplicates(["cust_id", "item_id"])

            # 上周和上上周哪些零售户都定了这款烟 并统计这款烟重购次数
            reorder_num = last_two_item.join(item, ["com_id","cust_id", "item_id"]) \
                .groupBy("com_id", "item_id") \
                .agg(f.count("cust_id").alias("reorder_num"))

            colName = "gauge_week_again"
            reorder_ratio = last_two_item.groupBy("com_id", "item_id") \
                .agg(f.count("cust_id").alias("total_num")) \
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
            # 零售户经纬度  零售户的订足率不用过滤掉档位为ZZ的
            co_cust = get_co_cust(spark).select("cust_id", "cust_name","cust_seg") \
                .join(get_cust_lng_lat(spark), "cust_id") \
                .withColumnRenamed("lng", "longitude") \
                .withColumnRenamed("lat", "latitude")

            print(f"{str(dt.now())} 全市各零售户该品规上周订足率热力分布")

            enouth_ratio = co_co_line.groupBy("com_id", "cust_id", "item_id") \
                .agg(f.sum(col("qty_ord")).alias("qty_ord"), f.sum(col("qty_rsn")).alias("qty_rsn")) \
                .withColumn(colName, col("qty_ord") / col("qty_rsn"))

            columns = ["city", "cust_id", "cust_name", "longitude", "latitude", "gauge_id", "gauge_name","gears",
                       "gears_data_marker", colName]
            enouth_ratio.join(city, "com_id") \
                .join(co_cust, "cust_id") \
                .join(plm_item, "item_id") \
                .withColumn("row", f.concat_ws("_", col("cust_id"), col("item_id"))) \
                .withColumn("gears_data_marker", f.lit("2")) \
                .withColumnRenamed("cust_seg","gears")\
                .withColumnRenamed("item_id", "gauge_id") \
                .withColumnRenamed("item_name", "gauge_name") \
                .foreachPartition(lambda x: write_hbase1(x, columns, hbase))
        except Exception:
            tb.print_exc()

        co_co_line.unpersist()
        co_cust.unpersist()
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
        .where(col("qty_rsn") > 0) \
        .select("sale_center_id", "item_id", "qty_ord", "qty_rsn","cust_id")

    #只按照档位过滤掉ZZ档位的，状态不过滤
    co_cust = spark.sql(
        "select cust_id from DB2_DB2INST1_CO_CUST where dt=(select max(dt) from DB2_DB2INST1_CO_CUST) and cust_seg!='ZZ'") \
        .where(col("com_id").isin(cities))


    try:
        print(f"{str(dt.now())} 各区县该卷烟品规订足率")
        colName = "county_gauge_reserve"
        reserve_ratio = co_co_line.join(co_cust,"cust_id")\
            .groupBy("sale_center_id", "item_id") \
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





def source_supply():
    #计算每个区每个档位对每款烟的评分

    try:
        print(f"{str(dt.now())} 每个区每个档位对每款烟的评分")
        co_cust=get_valid_co_cust(spark).select("com_id","cust_id","sale_center_id","cust_seg")\
                                        .where(col("cust_seg")!="ZZ")

        co_co_line=get_co_co_line(spark,scope=[0,30])\
                     .where((col("qty_ord")>0) & (col("qty_rsn")>0))\
                     .select("cust_id","item_id","qty_ord")
        #每个零售的每款烟的订单量
        qty_ord=co_co_line.groupBy("cust_id","item_id").agg(f.sum("qty_ord").alias("amount"))

        #每个零售户对每款卷烟的评分
        item_rating=get_cigar_rating(spark)

        # 各区县各档位零售户的数量
        cust_num = co_cust.groupBy("com_id","sale_center_id", "cust_seg") \
                          .agg(f.count("cust_id").alias("county_gears_volume_num"))
        #结果
        #区,档位,品规,评分,区各档位零售户数量
        result=qty_ord.join(item_rating,["cust_id","item_id"])\
               .withColumn("rating_amount",col("rating")*col("amount"))\
               .join(co_cust,"cust_id")\
               .groupBy("com_id","sale_center_id","cust_seg","item_id")\
               .agg(f.sum(col("rating_amount")).alias("county_gears_score"))\
               .join(cust_num,["com_id","sale_center_id","cust_seg"])



        plm_item = get_plm_item(spark).select("item_id", "item_name")

        area = get_area(spark)
        # com_id与city的映射关系
        city = area.dropDuplicates(["com_id"]).select("com_id", "city")
        # sale_center_id与区(list)的映射关系
        county = area.groupBy("sale_center_id") \
            .agg(f.collect_list("county").alias("county")) \
            .select("sale_center_id", "county")

        columns=["com_id","city","sale_center_id","county","gears","gauge_id","gauge_name",
                 "county_gears_score","county_gears_volume_num","gears_data_marker"]
        result.join(plm_item,"item_id")\
               .withColumn("row",f.concat_ws("_",col("sale_center_id"),col("cust_seg"),col("item_id")))\
               .join(city,"com_id")\
               .join(county,"sale_center_id")\
               .withColumn("gears_data_marker",f.lit("4"))\
               .withColumnRenamed("item_id","gauge_id")\
               .withColumnRenamed("item_name","gauge_name")\
               .withColumnRenamed("cust_seg","gears")\
               .foreachPartition(lambda x:write_hbase1(x,columns,hbase))
    except Exception:
        tb.print_exc()






def recommend_supply_value():
    #建议投放量

    try:
        print(f"{str(dt.now())} 建议投放量")
        spark.conf.set("spark.sql.execution.arrow.enabled", "true")
        co_co_line = get_co_co_line(spark, scope=[1, 365])\
                             .where(col("com_id")=="011114302")\
                             .select("co_num", "line_num", "cust_id", "item_id","sale_center_id",
                                     "qty_need", "qty_ord", "qty_rsn", "price", "amt","born_date")\
                             .withColumn("born_date",col("born_date").cast("string"))

        pd_df = co_co_line.toPandas()

        """
        item_id,2019-06-03 00:00:00
        1130309,51
        1130316,26
        """
        sub_df = predict(pd_df, 1)

        df = spark.createDataFrame(sub_df)

        value=df.columns[1]
        result = df.withColumnRenamed(value, "gauge_advise_volume") \
                     .withColumn("date", f.lit(value))



        plm_item=get_plm_item(spark).select("item_id","item_name")


        columns=["com_id","city","gauge_id","gauge_name","gauge_advise_volume","date","gears_data_marker"]
        result.join(plm_item,"item_id")\
            .withColumn("com_id",f.lit("011114302"))\
            .withColumn("city",f.lit("株洲市"))\
            .withColumn("row",f.concat_ws("_",col("com_id"),col("item_id")))\
            .withColumn("gears_data_marker",f.lit("0"))\
            .withColumnRenamed("item_id","gauge_id")\
            .withColumnRenamed("item_name","gauge_name")\
            .foreachPartition(lambda x:write_hbase1(x,columns,hbase))

    except Exception:
        tb.print_exc()