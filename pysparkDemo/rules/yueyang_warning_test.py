#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/5/8 14:37
import traceback as tb
from pyspark.sql import functions as f
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pysparkDemo.rules.write_hbase import write_hbase2
from pysparkDemo.rules.utils import *
from pyspark.sql import Window
from datetime import datetime as dt


spark = SparkSession.builder.enableHiveSupport().appName("yueyang_warning").getOrCreate()
sc=spark.sparkContext
sc.setLogLevel("WARN")

spark.sql("use aistrong")

hbase={"table":"TOBACCO.WARNING_CODE","families":["0"],"row":"cust_id"}
# hbase={"table":"test1","families":["0"],"row":"cust_id"}





#-----近30天零售户订货量与同等人流等级零售店订货量不符
def get_same_vfr_grade_excpet():
    print(f"{str(dt.now())}  近30天零售户订货量与同等人流等级零售店订货量不符")
    #获取每个零售户 city sale_center_id
    area_code = get_area(spark).dropDuplicates(["com_id", "sale_center_id"]).select("com_id", "sale_center_id", "city")
    co_cust = get_valid_co_cust(spark).select("cust_id", "com_id", "sale_center_id") \
                                      .join(area_code, ["com_id", "sale_center_id"])

    cols = {"value": "retail_month_orders",
            "level1_code": "classify_level1_code",
            }
    # 每个零售户近30天订货量
    qty_sum = get_co_co_01(spark, [0, 30]).where(col("com_id")=="011114306").groupBy("cust_id") \
        .agg(f.sum("qty_sum").alias(cols["value"]))



    # 每个零售户平均人流
    around_vfr = get_around_vfr(spark).where(col("city")=="岳阳市").withColumnRenamed("cust_id", "cust_id0")
    cust_cluster = get_cust_cluster(spark)


    around_vfr1 = around_vfr.withColumn("upper", col("avg_vfr") + col("avg_vfr") * 0.2) \
                            .withColumn("lower", col("avg_vfr") - col("avg_vfr") * 0.2) \
                            .withColumnRenamed("cust_id0", "cust_id1") \
                            .withColumnRenamed("city", "city1") \
                            .select("cust_id1", "city1", "upper", "lower")


    #同等人流等级零售户=全市其他零售户中当月平均人流位于该零售户当月平均人流正负20%范围的零售户
    # 与cust_id1 同人流级别的零售户cust_id0
    same_vfr_grade = around_vfr1.join(around_vfr, (col("city1") == col("city")) & (col("avg_vfr") > col("lower")) & (
                col("avg_vfr") < col("upper"))) \
                  .select("cust_id1", "cust_id0")

    result=same_vfr_grade.join(qty_sum,col("cust_id1")==col("cust_id"))\
                 .drop("cust_id")\
                 .withColumnRenamed(cols["value"],cols["value"]+"1")\
                 .join(qty_sum,col("cust_id0")==col("cust_id"))\
                 .drop("cust_id")\
                 .withColumnRenamed(cols["value"],cols["value"]+"0")
    result.write.csv("/user/entrobus/zhangzy/test/YJFL002",header=True,mode="overwrite")
#     except_cust = around_except_grade(same_vfr_grade, qty_sum,cust_cluster, cols, grade=[3, 4, 5]) \
#                                 .join(co_cust, "cust_id") \
#                                 .withColumn(cols["level1_code"], f.lit("YJFL002"))
#     values = [
#                  "avg_orders_plus3", "avg_orders_minu3", "avg_orders_plus4",
#                  "avg_orders_minu4", "avg_orders_plus5", "avg_orders_minu5",
#                  "warning_level_code", "cust_id", "city", "sale_center_id"
#              ] + list(cols.values())

#     except_cust.foreachPartition(lambda x: write_hbase2(x, values, hbase))

# get_same_vfr_grade_excpet()


# -----零售户近30天订货条均价与周边1km范围内零售户订货条均价不符
# -----零售户近30天订单额与周边1km范围内零售户订单额不符
def get_around_order_except():
    try:
        area_code = get_area(spark).dropDuplicates(["com_id", "sale_center_id"]).select("com_id", "sale_center_id",
                                                                                        "city")
        co_cust = get_valid_co_cust(spark).select("cust_id", "com_id", "sale_center_id") \
            .join(area_code, ["com_id", "sale_center_id"])

        co_co_01 = get_co_co_01(spark, scope=[0, 30]).where(col("com_id") == "011114306") \
            .select("qty_sum", "amt_sum", "cust_id")

        # 每个零售户的订货总量 总订货额
        qty_amt_sum = co_co_01.groupBy("cust_id") \
            .agg(f.sum("qty_sum").alias("order_sum"), f.sum("amt_sum").alias("amt_sum"))

        # 每个零售户cust_id1 周边有cust_id0这些零售户
        around_cust = get_around_cust(spark, 1).where(col("city1") == "岳阳市").select("cust_id1", "cust_id0")
        # 零售户聚类结果
        cust_cluster = get_cust_cluster(spark)

        print(f"{str(dt.now())}  零售户近30天订货条均价与周边1km范围内零售户订货条均价不符")
        try:
            cols = {"value": "retail_month_price",
                    "level1_code": "classify_level1_code",
                    }

            # 条均价
            avg_price = qty_amt_sum.withColumn(cols["value"], col("amt_sum") / col("order_sum")) \
                .select("cust_id", cols["value"])

            result = around_cust.join(avg_price, col("cust_id1") == col("cust_id")) \
                .drop("cust_id") \
                .withColumnRenamed(cols["value"], cols["value"] + "1") \
                .join(avg_price, col("cust_id0") == col("cust_id")) \
                .drop("cust_id") \
                .withColumnRenamed(cols["value"], cols["value"] + "0")
            result.write.csv("/user/entrobus/zhangzy/test/YJFL007", header=True,mode="overwrite")
        #             except_cust = around_except_grade(around_cust, avg_price,cust_cluster, cols, [3, 4, 5]) \
        #                                             .join(co_cust, "cust_id")\
        #                                             .withColumn(cols["level1_code"],f.lit("YJFL007"))

        #             values =[
        #                      "avg_orders_plus3","avg_orders_minu3","avg_orders_plus4",
        #                      "avg_orders_minu4","avg_orders_plus5","avg_orders_minu5",
        #                      "warning_level_code","cust_id", "city", "sale_center_id"
        #                     ]+list(cols.values())

        #             except_cust.foreachPartition(lambda x: write_hbase2(x, values, hbase))
        except Exception:
            tb.print_exc()

        print(f"{str(dt.now())} 零售户近30天订单额与周边1km范围内零售户订单额不符")
        try:
            cols = {"value": "retail_month_order_book",
                    "level1_code": "classify_level1_code",
                    }

            qty_amt_sum = qty_amt_sum.withColumnRenamed("amt_sum", cols["value"]) \
                .select("cust_id", cols["value"])

            result = around_cust.join(qty_amt_sum, col("cust_id1") == col("cust_id")) \
                .drop("cust_id") \
                .withColumnRenamed(cols["value"], cols["value"] + "1") \
                .join(qty_amt_sum, col("cust_id0") == col("cust_id")) \
                .drop("cust_id") \
                .withColumnRenamed(cols["value"], cols["value"] + "0")
            result.write.csv("/user/entrobus/zhangzy/test/YJFL009", header=True,mode="overwrite")
        #             except_cust = around_except_grade(around_cust, qty_amt_sum, cust_cluster,cols, [3, 4, 5]) \
        #                                             .join(co_cust, "cust_id")\
        #                                             .withColumn(cols["level1_code"],f.lit("YJFL009"))

        #             values = [
        #                      "avg_orders_plus3", "avg_orders_minu3", "avg_orders_plus4",
        #                      "avg_orders_minu4", "avg_orders_plus5", "avg_orders_minu5",
        #                      "warning_level_code", "cust_id", "city", "sale_center_id"
        #                      ] + list(cols.values())
        #             except_cust.foreachPartition(lambda x: write_hbase2(x, values, hbase))
        except Exception:
            tb.print_exc()
    except Exception:
        tb.print_exc()


# get_around_order_except()


# -----零售户高档烟订货比例与周边一公里范围内零售户订货比例不符
# -----零售户省内外烟订货金额比例与周边一公里范围内零售户省内外烟订货金额比例不符
# -----零售户省内外烟订货数量比例与周边一公里范围内零售户省内外烟订货数量比例不符
def get_around_item_except():
    area_code = get_area(spark).dropDuplicates(["com_id", "sale_center_id"]).select("com_id", "sale_center_id", "city")
    co_cust = get_valid_co_cust(spark).select("cust_id", "com_id", "sale_center_id") \
        .join(area_code, ["com_id", "sale_center_id"])

    co_co_line = get_co_co_line(spark, scope=[0, 30]).where(col("com_id") == "011114306") \
        .select("cust_id", "item_id", "qty_ord", "price", "amt")

    around_cust = get_around_cust(spark, 1).where(col("city1") == "岳阳市").select("cust_id1", "cust_id0")
    cust_cluster = get_cust_cluster(spark)

    print(f"{str(dt.now())}  零售户高档烟订货比例与周边一公里范围内零售户订货比例不符")
    try:
        cols = {"value": "retail_month_high",
                "level1_code": "classify_level1_code",
                }
        # 订货总量
        qty_sum = co_co_line.groupBy("cust_id").agg(f.sum("qty_ord").alias("qty_sum"))
        # 高价烟比例
        high_ratio = co_co_line.where(col("price") >= 500) \
            .groupBy("cust_id") \
            .agg(f.sum("qty_ord").alias("high_qty_sum")) \
            .join(qty_sum, "cust_id") \
            .withColumn(cols["value"], col("high_qty_sum") / col("qty_sum")) \
            .select("cust_id", cols["value"])

        result = around_cust.join(high_ratio, col("cust_id1") == col("cust_id")) \
            .drop("cust_id") \
            .withColumnRenamed(cols["value"], cols["value"] + "1") \
            .join(high_ratio, col("cust_id0") == col("cust_id")) \
            .drop("cust_id") \
            .withColumnRenamed(cols["value"], cols["value"] + "0")

        result.write.csv("/user/entrobus/zhangzy/test/YJFL008", header=True,mode="overwrite")
    #         grade = [3, 4, 5]
    #         result = around_except_grade(around_cust, high_ratio, cust_cluster,cols, grade) \
    #                                     .join(co_cust, "cust_id") \
    #                                     .withColumn(cols["level1_code"], f.lit("YJFL008"))

    #         values = [
    #                  "avg_orders_plus3", "avg_orders_minu3", "avg_orders_plus4",
    #                  "avg_orders_minu4", "avg_orders_plus5", "avg_orders_minu5",
    #                  "warning_level_code", "cust_id", "city", "sale_center_id"
    #                  ] + list(cols.values())
    #         result.foreachPartition(lambda x: write_hbase2(x, values, hbase))
    except Exception:
        tb.print_exc()

    # 零售户省内外烟订货金额比例=省内烟订货金额/省外烟订货金额
    # 零售户省内外烟订货比例 = 省内烟订货数量 / 省外烟订货数量
    try:
        # 金额
        cols1 = {"value": "month_amount_ratio",
                 "level1_code": "classify_level1_code",
                 }
        # 数量
        cols2 = {"value": "month_count_ratio",
                 "level1_code": "classify_level1_code",
                 }

        line_plm = get_plm_item(spark).select("item_id", "yieldly_type") \
            .join(co_co_line, "item_id")

        # 零售户所定省内烟 数量 订货金额
        in_prov = line_plm.where(col("yieldly_type") == "0") \
            .groupBy("cust_id") \
            .agg(f.sum("qty_ord").alias("in_prov_qty"), f.sum("amt").alias("in_prov_amt"))
        # 零售户所定省外烟 数量 订货金额
        out_prov = line_plm.where(col("yieldly_type") == "1") \
            .groupBy("cust_id") \
            .agg(f.sum("qty_ord").alias("out_prov_qty"), f.sum("amt").alias("out_prov_amt"))

        # 省内外烟  订货数量比例  订货金额比例
        in_out_prov_ratio = in_prov.join(out_prov, "cust_id") \
            .withColumn(cols2["value"], col("in_prov_qty") / col("out_prov_qty")) \
            .withColumn(cols1["value"], col("in_prov_amt") / col("out_prov_amt")) \
            .select(cols1["value"], cols2["value"], "cust_id")

        print(f"{str(dt.now())}  零售户省内外烟订货金额比例与周边一公里范围内零售户省内外烟订货金额比例不符")
        # 省内外烟  订货金额比例 异常

        result = around_cust.join(in_out_prov_ratio.drop(cols2["value"]), col("cust_id1") == col("cust_id")) \
            .drop("cust_id") \
            .withColumnRenamed(cols1["value"], cols1["value"] + "1") \
            .join(in_out_prov_ratio.drop(cols2["value"]), col("cust_id0") == col("cust_id")) \
            .drop("cust_id") \
            .withColumnRenamed(cols1["value"], cols1["value"] + "0")

        result.write.csv("/user/entrobus/zhangzy/test/YJFL010", header=True,mode="overwrite")

        #         result=around_except_grade(around_cust, in_out_prov_ratio,cust_cluster, cols1, grade)\
        #                  .join(co_cust, "cust_id") \
        #                  .withColumn(cols1["level1_code"], f.lit("YJFL010"))

        #         values = [
        #                  "avg_orders_plus3", "avg_orders_minu3", "avg_orders_plus4",
        #                  "avg_orders_minu4", "avg_orders_plus5", "avg_orders_minu5",
        #                  "warning_level_code", "cust_id", "city", "sale_center_id"
        #                  ] + list(cols1.values())
        #         result.foreachPartition(lambda x: write_hbase2(x, values, hbase))

        print(f"{str(dt.now())}  零售户省内外烟订货数量比例与周边一公里范围内零售户省内外烟订货数量比例不符")
        # 省内外烟  订货数量比例 异常
        result = around_cust.join(in_out_prov_ratio.drop(cols1["value"]), col("cust_id1") == col("cust_id")) \
            .drop("cust_id") \
            .withColumnRenamed(cols2["value"], cols2["value"] + "1") \
            .join(in_out_prov_ratio.drop(cols1["value"]), col("cust_id0") == col("cust_id")) \
            .drop("cust_id") \
            .withColumnRenamed(cols2["value"], cols2["value"] + "0")

        result.write.csv("/user/entrobus/zhangzy/test/YJFL011", header=True,mode="overwrite")
    #         result=around_except_grade(around_cust, in_out_prov_ratio,cust_cluster, cols2, grade) \
    #                         .join(co_cust, "cust_id") \
    #                         .withColumn(cols2["level1_code"], f.lit("YJFL011"))

    #         values = [
    #                  "avg_orders_plus3", "avg_orders_minu3", "avg_orders_plus4",
    #                  "avg_orders_minu4", "avg_orders_plus5", "avg_orders_minu5",
    #                  "warning_level_code", "cust_id", "city", "sale_center_id"
    #                  ] + list(cols2.values())
    #         result.foreachPartition(lambda x: write_hbase2(x, values, hbase))
    except Exception:
        tb.print_exc()


# get_around_item_except()















# -----近30天零售户订货量与当月周边人流数量不符
def get_qty_vfr_except():
    print(f"{str(dt.now())}  近30天零售户订货量与当月周边人流数量不符")
    area_code = get_area(spark).dropDuplicates(["com_id", "sale_center_id"]).select("com_id", "sale_center_id", "city")
    co_cust = get_valid_co_cust(spark).select("cust_id", "com_id", "sale_center_id") \
        .join(area_code, ["com_id", "sale_center_id"])


    cust_cluster = get_cust_cluster(spark)

    # 零售户近30天订货量
    qty_sum = get_co_co_01(spark, [0, 30]).where(col("com_id") == "011114306").groupBy("cust_id") \
        .agg(f.sum("qty_sum").alias("qty_sum"))

    # 获取周边人流
    around_vfr = get_around_vfr(spark).where(col("city") == "岳阳市")

    cols = {"value": "stream_avg_orders",
            "level1_code": "classify_level1_code",
            }
    result = qty_sum.join(around_vfr, "cust_id") \
        .withColumn(cols["value"], col("qty_sum") / col("avg_vfr")) \
        .join(cust_cluster,"cust_id")\
        .select("city","cust_id", "cluster_index",cols["value"])


    result.write.csv("/user/entrobus/zhangzy/test/YJFL001", header=True,mode="overwrite")


# get_qty_vfr_except()


#-----近30天零售户档位与周边消费水平不符
def get_grade_cons_except():
    print(f"{str(dt.now())} 近30天零售户档位与周边消费水平不符")
    try:
        consume_level=get_all_consume_level(spark)\
                             .where(col("city")=="岳阳市")\
                             .select("city","cust_id","consume_level")
        cust_cluster=get_cust_cluster(spark)

        #只展示目前有效的零售户    去掉 过去有效 现在无效的零售户
        valid_co_cust=get_valid_co_cust(spark).select("cust_id","sale_center_id")

        #近30天零售户档位  一个零售户近30天内多种档位，分别预警
        cust_seg=spark.sql("select * from DB2_DB2INST1_CO_CUST")\
             .where(f.datediff(f.current_date(),col("dt"))<=30)\
             .where(col("com_id").isin(["011114306"]))\
             .withColumn("cust_seg",f.regexp_replace("cust_seg","ZZ","31"))\
             .select("com_id","cust_id","cust_seg")\
             .join(valid_co_cust,"cust_id")\
             .dropDuplicates(["cust_id","cust_seg"])

        cols = {"value": "retail_rim_cons",
                "level1_code": "classify_level1_code",
                }
        cust_seg_cons=cust_seg.join(consume_level,"cust_id")\
                             .withColumnRenamed("consume_level",cols["value"])\
                             .join(cust_cluster,"cust_id")\
                             .select("city","cust_id","cust_seg","cluster_index","sale_center_id",cols["value"])
        cust_seg_cons.select("city","cust_id","cust_seg",cols["value"])\
                    .write.csv("/user/entrobus/zhangzy/test/YJFL003",header=True,mode="overwrite")
#         result=except_grade(cust_seg_cons,cols,["city","cust_seg","cluster_index"],[2,3,4])\
#                 .withColumnRenamed("cust_seg","retail_grade")\
#                 .withColumnRenamed("mean","city_grade_cons_avg") \
#                 .withColumn(cols["level1_code"], f.lit("YJFL003"))

#         values = [
#                  "avg_orders_plus3", "avg_orders_minu3", "avg_orders_plus4",
#                  "avg_orders_minu4", "avg_orders_plus5", "avg_orders_minu5",
#                  "warning_level_code", "cust_id", "city", "sale_center_id",
#                  "retail_grade", "city_grade_cons_avg"
#                  ] + list(cols.values())
#         result.foreachPartition(lambda x:write_hbase2(x,values,hbase))
    except Exception:
        tb.print_exc()
# get_grade_cons_except()


# -----近30天零售户高档烟订货比例与周边消费水平不符
def get_high_cons_except():
    try:
        cols = {"value": "high_order_cons_ratio",
                "level1_code": "classify_level1_code",
                }

        co_cust = get_valid_co_cust(spark).select("cust_id","sale_center_id")\



        cust_cluster = get_cust_cluster(spark)

        print(f"{str(dt.now())}  零售店高价烟比例/消费水平")
        # 消费水平
        consume_level_df = get_all_consume_level(spark).select("city", "cust_id", "consume_level")


        co_co_line = get_co_co_line(spark, scope=[0, 30]) \
                               .select("cust_id", "qty_ord", "price")


        # 零售户所定烟的总数目
        item_num = co_co_line.groupBy("cust_id").agg(f.sum("qty_ord").alias("item_num"))

        # 每个零售户高价烟的数量
        high_price_num = co_co_line.where(col("price") >= 500) \
            .groupBy("cust_id") \
            .agg(f.sum("qty_ord").alias("high_price_num"))
        # 每个零售户高价烟比例
        high_price_ratio = item_num.join(high_price_num, "cust_id") \
            .withColumn("high_price_ratio", col("high_price_num") / col("item_num")) \
            .select("cust_id", "high_price_ratio")

        # 每个零售户高档烟比例/周边消费水平的比值
        high_consume_level = high_price_ratio.join(consume_level_df, "cust_id") \
            .withColumn(cols["value"], col("high_price_ratio") / col("consume_level")) \
            .join(cust_cluster,"cust_id")\
            .select("city", "cust_id","cluster_index", cols["value"])

        high_consume_level.select("city","cust_id",cols["value"])\
                    .write.csv("/user/entrobus/zhangzy/test/YJFL004",header=True,mode="overwrite")
#         result=except_grade(high_consume_level, cols, ["city","cluster_index"], [3, 4, 5]) \
#                                         .join(co_cust, "cust_id") \
#                                         .withColumn(cols["level1_code"], f.lit("YJFL004"))

#         values =[
#               "avg_orders_plus3", "avg_orders_minu3", "avg_orders_plus4",
#               "avg_orders_minu4", "avg_orders_plus5", "avg_orders_minu5",
#               "warning_level_code", "cust_id", "city", "sale_center_id"
#               ] + list(cols.values())
#         result.foreachPartition(lambda x: write_hbase2(x, values, hbase))
    except Exception:
        tb.print_exc()

# get_high_cons_except()




# -----近30天零售户订货条均价与周边消费水平不符
# -----近30天零售户订单额与周边消费水平不符
def get_avg_cons_except():
    try:

        co_cust = get_valid_co_cust(spark).select("cust_id", "sale_center_id")\

        consume_level_df = get_all_consume_level(spark).select("city", "cust_id", "consume_level")

        cust_cluster = get_cust_cluster(spark)


        # -----------------------获取co_co_01
        co_co_01 = get_co_co_01(spark, scope=[0, 30]) \
                               .select("qty_sum", "amt_sum", "cust_id")

        # 每个零售户的订货总量 总订货额
        qty_amt_sum = co_co_01.groupBy("cust_id") \
                              .agg(f.sum("qty_sum").alias("order_sum"), f.sum("amt_sum").alias("amt_sum"))

        print(f"{str(dt.now())}  零售店订货条均价/消费水平")
        try:
            cols = {"value": "order_price_cons",
                    "level1_code": "classify_level1_code",
                    }
            # 每个零售户的订货条均价
            avg_price = qty_amt_sum.withColumn("avg_price", col("amt_sum") / col("order_sum")) \
                                  .select("cust_id", "avg_price")

            avg_consume_level = avg_price.join(consume_level_df, "cust_id") \
                                        .withColumn(cols["value"], col("avg_price") / col("consume_level")) \
                                        .join(cust_cluster,"cust_id")\
                                        .select("city", "cust_id","cluster_index", cols["value"])


            avg_consume_level.select("city","cust_id",cols["value"])\
                              .write.csv("/user/entrobus/zhangzy/test/YJFL006",header=True,mode="overwrite")
#             result=except_grade(avg_consume_level, cols, ["city","cluster_index"], [3, 4, 5]) \
#                 .join(co_cust, "cust_id") \
#                 .withColumn(cols["level1_code"], f.lit("YJFL006"))

#             values = [
#                      "avg_orders_plus3", "avg_orders_minu3", "avg_orders_plus4",
#                      "avg_orders_minu4", "avg_orders_plus5", "avg_orders_minu5",
#                      "warning_level_code", "cust_id", "city", "sale_center_id"
#                      ] + list(cols.values())
#             result.foreachPartition(lambda x: write_hbase2(x, values, hbase))
        except Exception:
            tb.print_exc()

        print(f"{str(dt.now())}  零售店订单额/消费水平")
        try:
            cols = {"value": "order_book_cons",
                    "level1_code": "classify_level1_code",
                    }

            amt_cons_ratio = qty_amt_sum.join(consume_level_df, "cust_id") \
                                        .withColumn(cols["value"], col("amt_sum") / col("consume_level")) \
                                        .join(cust_cluster,"cust_id")\
                                        .select("city", "cust_id","cluster_index", cols["value"])


            amt_cons_ratio.select("city","cust_id",cols["value"])\
                              .write.csv("/user/entrobus/zhangzy/test/YJFL005",header=True,mode="overwrite")
#             result=except_grade(amt_cons_ratio, cols, ["city","cluster_index"], [3, 4, 5])\
#                 .join(co_cust, "cust_id") \
#                 .withColumn(cols["level1_code"], f.lit("YJFL005"))

#             values = [
#                      "avg_orders_plus3", "avg_orders_minu3", "avg_orders_plus4",
#                      "avg_orders_minu4", "avg_orders_plus5", "avg_orders_minu5",
#                      "warning_level_code", "cust_id", "city", "sale_center_id"
#                      ] + list(cols.values())
#             result.foreachPartition(lambda x: write_hbase2(x, values, hbase))
        except Exception:
            tb.print_exc()
    except Exception:
        tb.print_exc()

# get_avg_cons_except()


