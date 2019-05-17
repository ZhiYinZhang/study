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


spark = SparkSession.builder.enableHiveSupport().appName("retail warning").getOrCreate()
sc=spark.sparkContext
sc.setLogLevel("WARN")

spark.sql("use aistrong")

hbase={"table":"TOBACCO.RETAIL_WARNING","families":["0"],"row":"cust_id"}
# hbase={"table":"test1","families":["0"],"row":"cust_id"}


# -----零售户近30天订货条均价与周边1km范围内零售户订货条均价不符
# -----零售户近30天订单额与周边1km范围内零售户订单额不符
def get_around_order_except():
    try:
        area_code = get_area(spark).dropDuplicates(["com_id", "sale_center_id"]).select("com_id", "sale_center_id","city")
        co_cust = get_valid_co_cust(spark).select("cust_id", "com_id", "sale_center_id") \
            .join(area_code, ["com_id", "sale_center_id"])

        # -----------------------获取co_co_01
        co_co_01 = get_co_co_01(spark, scope=[0, 30]) \
            .select("qty_sum", "amt_sum", "cust_id")

        # 每个零售户的订货总量 总订货额
        qty_amt_sum = co_co_01.groupBy("cust_id") \
            .agg(f.sum("qty_sum").alias("order_sum"), f.sum("amt_sum").alias("amt_sum"))

        # 每个零售户cust_id1 周边有cust_id0这些零售户
        around_cust = get_around_cust(spark, 1)

        print(f"{str(dt.now())}  零售户近30天订货条均价与周边1km范围内零售户订货条均价不符")
        try:
            cols = {"value": "retail_month_price",
                    "abnormal": "km_discrepancy_price",
                    'plus_one_grade': 'retail_month_price_plus3',
                    'minus_one_grade': 'retail_month_price_minu3',
                    'plus_two_grade': 'retail_month_price_plus4',
                    'minus_two_grade': 'retail_month_price_minu4',
                    'plus_three_grade': 'retail_month_price_plus5',
                    'minus_three_grade': 'retail_month_price_minu5'
                    }

            # 条均价
            avg_price = qty_amt_sum.withColumn(cols["value"], col("amt_sum") / col("order_sum")) \
                .select("cust_id", cols["value"])

            except_cust = around_except_grade(around_cust, avg_price, cols, [3, 4, 5]) \
                .join(co_cust, "cust_id")

            values = list(cols.values()) + ["cust_id", "city", "sale_center_id"]
            except_cust.foreachPartition(lambda x: write_hbase2(x, values, hbase))
        except Exception:
            tb.print_exc()

        print(f"{str(dt.now())} 零售户近30天订单额与周边1km范围内零售户订单额不符")
        try:
            cols = {"value": "retail_month_order_book",
                    "abnormal": "km_discrepancy_order_book",
                    'plus_one_grade': 'retail_month_order_book_plus3',
                    'minus_one_grade': 'retail_month_order_book_minu3',
                    'plus_two_grade': 'retail_month_order_book_plus4',
                    'minus_two_grade': 'retail_month_order_book_minu4',
                    'plus_three_grade': 'retail_month_order_book_plus5',
                    'minus_three_grade': 'retail_month_order_book_minu5'
                    }

            qty_amt_sum = qty_amt_sum.withColumnRenamed("amt_sum", cols["value"])
            except_cust = around_except_grade(around_cust, qty_amt_sum, cols, [3, 4, 5]) \
                .join(co_cust, "cust_id")

            values = list(cols.values()) + ["cust_id", "city", "sale_center_id"]

            except_cust.foreachPartition(lambda x: write_hbase2(x, values, hbase))
        except Exception:
            tb.print_exc()
    except Exception:
        tb.print_exc()




#-----零售户高档烟订货比例与周边一公里范围内零售户订货比例不符
#-----零售户省内外烟订货金额比例与周边一公里范围内零售户省内外烟订货金额比例不符
#-----零售户省内外烟订货数量比例与周边一公里范围内零售户省内外烟订货数量比例不符
def get_around_item_except():
    area_code=get_area(spark).dropDuplicates(["com_id","sale_center_id"]).select("com_id","sale_center_id","city")
    co_cust=get_valid_co_cust(spark).select("cust_id","com_id","sale_center_id")\
                                          .join(area_code,["com_id","sale_center_id"])

    co_co_line = get_co_co_line(spark,scope=[0,30])\
                    .select("cust_id", "item_id", "qty_ord", "price", "amt")

    around_cust = get_around_cust(spark, 1)



    print(f"{str(dt.now())}  零售户高档烟订货比例与周边一公里范围内零售户订货比例不符")
    try:
        cols = {"value": "retail_month_high",
                "abnormal": "km_discrepancy_high_ratio",
                'plus_one_grade': 'retail_month_high_plus3',
                'minus_one_grade': 'retail_month_high_minu3',
                'plus_two_grade': 'retail_month_high_plus4',
                'minus_two_grade': 'retail_month_high_minu4',
                'plus_three_grade': 'retail_month_high_plus5',
                'minus_three_grade': 'retail_month_high_minu5'
                }
        #订货总量
        qty_sum = co_co_line.groupBy("cust_id").agg(f.sum("qty_ord").alias("qty_sum"))
        #高价烟比例
        high_ratio = co_co_line.where(col("price") >= 500) \
            .groupBy("cust_id") \
            .agg(f.sum("qty_ord").alias("high_qty_sum")) \
            .join(qty_sum, "cust_id") \
            .withColumn(cols["value"], col("high_qty_sum")/col("qty_sum"))

        grade = [3, 4, 5]
        except_cust = around_except_grade(around_cust, high_ratio, cols, grade).join(co_cust, "cust_id")

        values = list(cols.values())+["cust_id","city","sale_center_id"]
        except_cust.foreachPartition(lambda x: write_hbase2(x, values, hbase))
    except Exception:
        tb.print_exc()


    #零售户省内外烟订货金额比例=省内烟订货金额/省外烟订货金额
    try:
        #金额
        cols1 = {"value": "month_amount_ratio",
                 "abnormal": "amount_ratio_discrepancy_outprov",
                 'plus_one_grade': 'month_amount_ratio_plus3',
                 'minus_one_grade': 'month_amount_ratio_minu3',
                 'plus_two_grade': 'month_amount_ratio_plus4',
                 'minus_two_grade': 'month_amount_ratio_minu4',
                 'plus_three_grade': 'month_amount_ratio_plus5',
                 'minus_three_grade': 'month_amount_ratio_minu5'
                 }
        #数量
        cols2 = {"value": "month_count_ratio",
                 "abnormal": "count_ratio_discrepancy_inprov",
                 'plus_one_grade': 'month_count_ratio_plus3',
                 'minus_one_grade': 'month_count_ratio_minu3',
                 'plus_two_grade': 'month_count_ratio_plus4',
                 'minus_two_grade': 'month_count_ratio_minu4',
                 'plus_three_grade': 'month_count_ratio_plus5',
                 'minus_three_grade': 'month_count_ratio_minu5'
                 }

        line_plm = get_plm_item(spark).select("item_id", "yieldly_type")\
                                      .join(co_co_line,"item_id")

        # 零售户所定省内烟 数量 订货金额
        in_prov = line_plm.where(col("yieldly_type") == "0") \
            .groupBy("cust_id") \
            .agg(f.sum("qty_ord").alias("in_prov_qty"), f.sum("amt").alias("in_prov_amt"))
        # 零售户所定省外烟 数量 订货金额
        out_prov = line_plm.where(col("yieldly_type") == "1") \
            .groupBy("cust_id") \
            .agg(f.sum("qty_ord").alias("out_prov_qty"), f.sum("amt").alias("out_prov_amt"))


        #省内外烟  订货数量比例  订货金额比例
        in_out_prov_ratio = in_prov.join(out_prov, "cust_id") \
            .withColumn(cols2["value"], col("in_prov_qty") / col("out_prov_qty")) \
            .withColumn(cols1["value"], col("in_prov_amt") / col("out_prov_amt"))


        print(f"{str(dt.now())}  零售户省内外烟订货金额比例与周边一公里范围内零售户省内外烟订货金额比例不符")
        #省内外烟  订货金额比例 异常
        values = list(cols1.values())+["cust_id","city","sale_center_id"]
        around_except_grade(around_cust, in_out_prov_ratio, cols1, grade)\
                 .join(co_cust, "cust_id") \
                 .foreachPartition(lambda x: write_hbase2(x, values, hbase))


        print(f"{str(dt.now())}  零售户省内外烟订货数量比例与周边一公里范围内零售户省内外烟订货数量比例不符")
        #省内外烟  订货数量比例 异常
        values = list(cols2.values())+["cust_id","city","sale_center_id"]
        around_except_grade(around_cust, in_out_prov_ratio, cols2, grade)\
                        .join(co_cust, "cust_id") \
                        .foreachPartition(lambda x: write_hbase2(x, values, hbase))
    except Exception:
        tb.print_exc()










#-----近30天零售户消费水平与全市同档位消费水平不符
def get_grade_cons_except():
    print(f"{str(dt.now())} 近30天零售户消费水平与全市同档位消费水平不符")
    try:
        consume_level=get_consume_level(spark)\
                             .select("city","cust_id","consume_level")
        #只展示目前有效的零售户    可以去掉 过去有效 现在无效的零售户
        valid_co_cust=get_valid_co_cust(spark).select("cust_id","sale_center_id")

        cust_seg=spark.sql("select * from DB2_DB2INST1_CO_CUST")\
             .where(f.datediff(f.current_date(),col("dt"))<=30)\
             .where(col("com_id").isin(["011114305","011114306","011114302"]))\
             .withColumn("cust_seg",f.regexp_replace("cust_seg","ZZ","31"))\
             .select("com_id","cust_id","cust_seg")\
             .join(valid_co_cust,"cust_id")\
             .dropDuplicates(["cust_id","cust_seg"])

        cols = {"value": "retail_rim_cons",
                        "abnormal": "grade_discrepancy_cons_month",
                        'plus_one_grade': 'city_grade_cons_avg_plus2',
                        'minus_one_grade': 'city_grade_cons_avg_minu2',
                        'plus_two_grade': 'city_grade_cons_avg_plus3',
                        'minus_two_grade': 'city_grade_cons_avg_minu3',
                        'plus_three_grade': 'city_grade_cons_avg_plus4',
                        'minus_three_grade': 'city_grade_cons_avg_minu4'
                        }
        cust_seg_cons=cust_seg.join(consume_level,"cust_id")\
                             .withColumnRenamed("consume_level",cols["value"])\
                             .select("city","cust_id","cust_seg","sale_center_id",cols["value"])

        except_cust=except_grade(cust_seg_cons,cols,["city","cust_seg"],[2,3,4])
        cols1=["retail_grade","city_grade_cons_avg","city","sale_center_id","cust_id"]
        values=list(cols.values())+cols1
        except_cust.withColumnRenamed("cust_seg",cols1[0])\
                .withColumnRenamed("mean",cols1[1])\
                .foreachPartition(lambda x:write_hbase2(x,values,hbase))
    except Exception:
        tb.print_exc()





# -----近30天零售户高档烟订货比例与周边消费水平不符
def get_high_cons_except():
    try:
        cols = {"value": "high_order_cons_ratio",
                "abnormal": "high_discrepancy_cons_month",
                'plus_one_grade': 'high_order_cons_ratio_plus3',
                'minus_one_grade': 'high_order_cons_ratio_minu3',
                'plus_two_grade': 'high_order_cons_ratio_plus4',
                'minus_two_grade': 'high_order_cons_ratio_minu4',
                'plus_three_grade': 'high_order_cons_ratio_plus5',
                'minus_three_grade': 'high_order_cons_ratio_minu5'
                }

        co_cust = get_valid_co_cust(spark).select("cust_id", "sale_center_id")
        print(f"{str(dt.now())}  零售店高价烟比例/消费水平")
        # 消费水平
        consume_level_df = get_consume_level(spark).select("city", "cust_id", "consume_level")

        # -----------------------获取co_co_line
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
            .select("city", "cust_id", cols["value"])

        values = list(cols.values()) + ["cust_id", "city", "sale_center_id"]
        except_grade(high_consume_level, cols, ["city"], [3, 4, 5]) \
            .join(co_cust, "cust_id") \
            .foreachPartition(lambda x: write_hbase2(x, values, hbase))
    except Exception:
        tb.print_exc()





# -----近30天零售户订货条均价与周边消费水平不符
# -----近30天零售户订单额与周边消费水平不符
def get_avg_cons_except():
    try:
        co_cust = get_valid_co_cust(spark).select("cust_id", "sale_center_id")

        consume_level_df = get_consume_level(spark).select("city", "cust_id", "consume_level")

        # -----------------------获取co_co_01
        co_co_01 = get_co_co_01(spark, scope=[0, 30]) \
            .select("qty_sum", "amt_sum", "cust_id")

        # 每个零售户的订货总量 总订货额
        qty_amt_sum = co_co_01.groupBy("cust_id") \
            .agg(f.sum("qty_sum").alias("order_sum"), f.sum("amt_sum").alias("amt_sum"))

        print(f"{str(dt.now())}  零售店订货条均价/消费水平")
        try:
            cols = {"value": "order_price_cons",
                    "abnormal": "price_discrepancy_cons_month",
                    'plus_one_grade': 'order_price_cons_plus3',
                    'minus_one_grade': 'order_price_cons_minu3',
                    'plus_two_grade': 'order_price_cons_plus4',
                    'minus_two_grade': 'order_price_cons_minu4',
                    'plus_three_grade': 'order_price_cons_plus5',
                    'minus_three_grade': 'order_price_cons_minu5'
                    }
            # 每个零售户的订货条均价
            avg_price = qty_amt_sum.withColumn("avg_price", col("amt_sum") / col("order_sum")) \
                .select("cust_id", "avg_price")

            avg_consume_level = avg_price.join(consume_level_df, "cust_id") \
                .withColumn(cols["value"], col("avg_price") / col("consume_level")) \
                .select("city", "cust_id", cols["value"])

            values = list(cols.values()) + ["city", "cust_id", "sale_center_id"]

            except_grade(avg_consume_level, cols, ["city"], [3, 4, 5]) \
                .join(co_cust, "cust_id") \
                .foreachPartition(lambda x: write_hbase2(x, values, hbase))
        except Exception:
            tb.print_exc()

        print(f"{str(dt.now())}  零售店订单额/消费水平")
        try:
            cols = {"value": "order_book_cons",
                    "abnormal": "order_book_discrepancy_cons",
                    'plus_one_grade': 'order_book_cons_plus3',
                    'minus_one_grade': 'order_book_cons_minu3',
                    'plus_two_grade': 'order_book_cons_plus4',
                    'minus_two_grade': 'order_book_cons_minu4',
                    'plus_three_grade': 'order_book_cons_plus5',
                    'minus_three_grade': 'order_book_cons_minu5'
                    }

            amt_cons_ratio = qty_amt_sum.join(consume_level_df, "cust_id") \
                .withColumn(cols["value"], col("amt_sum") / col("consume_level")) \
                .select("city", "cust_id", cols["value"])

            values = list(cols.values()) + ["city", "cust_id", "sale_center_id"]
            print(values)
            except_grade(amt_cons_ratio, cols, ["city"], [3, 4, 5]) \
                .join(co_cust, "cust_id") \
                .foreachPartition(lambda x: write_hbase2(x, values, hbase))
        except Exception:
            tb.print_exc()
    except Exception:
        tb.print_exc()