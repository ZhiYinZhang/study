#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/4/8 15:29
import traceback as tb
from pyspark.sql.functions import col
from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pysparkDemo.rules_temp.write_hbase import write_hbase1
from datetime import datetime as dt
from pysparkDemo.rules_temp.utils import *

spark = SparkSession.builder.enableHiveSupport().appName("area").getOrCreate()
sc=spark.sparkContext
sc.setLogLevel("WARN")

spark.sql("use aistrong")

#--------------------------------------------area表（区域画像）-----------------------------------------------------
hbase={"table":"TOBACCO.AREA","families":["0"],"row":"sale_center_id"}
hbase={"table":"test_ma","families":["info"],"row":"sale_center_id"}


def get_order_stats_info():
    # -----------------------获取co_co_01     unique_kind: 90 退货  10 普通订单    pmt_status:  0 未付款  1 收款完成
    try:
        colNames=["qty_sum","amt_sum","sale_center_id","born_date","today"]
        co_co_01=get_co_co_01(spark).select(colNames) \
            .withColumn("week_diff", week_diff("today", "born_date")) \
            .withColumn("month_diff",month_diff_udf(f.year(col("born_date")), f.month(col("born_date")), f.year(col("today")),f.month(col("today")))) \
            .where(col("month_diff") <= 2)


        #上一周 上两周 上四周 上个月 本月
        days = [1, 2, 4, 1, 0]

        cols0={"sum":["sum_last_week","sum_last_two_week","sum_last_four_week","sum_last_month","sum_this_month"],
               "amount":["amount_last_week","amount_last_two_week","amount_last_four_week","amount_last_month","amount_this_month"],
               "price":["price_last_week","price_last_two_week","price_last_four_week","price_last_month","price_this_month"],
               "orders":["orders_last_week","orders_last_two_week","orders_last_four_week","orders_last_month","orders_this_month"],

               "ring_sum":["sum_ring_last_week","sum_ring_last_two_week","sum_ring_last_four_week","sum_ring_last_month"],
               "ring_amount":["amount_ring_last_week","amount_ring_last_two_week","amount_ring_last_four_week","amount_ring_last_month"],
               "ring_price":["price_ring_last_week","price_ring_last_two_week","price_ring_last_four_week","price_ring_last_month"],
               "ring_orders":["orders_ring_last_week","orders_ring_last_two_week","orders_ring_last_four_week","orders_ring_last_month"]
               }

        for i in range(len(days)):
            day=days[i]
            sum_colName = cols0["sum"][i]
            amount_colName = cols0["amount"][i]
            price_colName = cols0["price"][i]
            orders_colName = cols0["orders"][i]

            if i<3:
                day_filter = co_co_01.where((col("week_diff")>0) & (col("week_diff") <= day))
            else:
                day_filter = co_co_01.where(col("month_diff") == day)

            # 本县或区所有零售户上一周 / 上两周 / 上四周 / 上个月 / 本月截止订货总量
            print(f"{str(dt.now())}   order sum : {day}")
            try:
                order_sum = day_filter.select("sale_center_id", "qty_sum") \
                    .groupBy("sale_center_id") \
                    .agg(f.sum("qty_sum").alias(sum_colName))
                order_sum.foreachPartition(lambda x: write_hbase1(x, [sum_colName],hbase))
            except Exception:
                tb.print_exc()


            # 本县或区所有零售户上一周 / 上两周 / 上四周 / 上个月 / 本月截止订货金额总和
            print(f"{str(dt.now())}   amount sum : {day}")
            try:
                amount_total = day_filter.select("sale_center_id", "amt_sum") \
                    .groupBy("sale_center_id") \
                    .agg(f.sum("amt_sum").alias(amount_colName))
                amount_total.foreachPartition(lambda x: write_hbase1(x, [amount_colName],hbase))
            except Exception:
                tb.print_exc()


            # 本县或区所有零售户上一周 / 上两周 / 上四周 / 上个月订货条均价
            print(f"{str(dt.now())}   avg price : {day}")
            try:
                avg_price = order_sum.join(amount_total, "sale_center_id") \
                    .withColumn(price_colName, divider_udf(col(amount_colName), col(sum_colName)))
                avg_price.foreachPartition(lambda x: write_hbase1(x, [price_colName],hbase))
            except Exception:
                tb.print_exc()


            # 本县或区所有零售户上一周 / 上两周 / 上四周 / 上个月订单数总和
            print(f"{str(dt.now())}   order count : {day}")
            try:
                order_count=day_filter.groupBy("sale_center_id") \
                    .count() \
                    .withColumnRenamed("count", orders_colName)
                order_count.foreachPartition(lambda x: write_hbase1(x, [orders_colName],hbase))
            except Exception:
                tb.print_exc()


            #--------------------------------------环比------------------------------------------
            # 上一周 上两周 上四周 上个月 环比情况
            # (1,2] (1,3] (1,5] (1,2]
            if i < 4:
                if i < 3:
                    day_filter = co_co_01.where((col("week_diff") > 1) & (col("week_diff") <= (1 + day)))
                else:
                    day_filter = co_co_01.where((col("month_diff") > 1) & (col("month_diff") <= (1 + day)))
                try:
                    # 本县或区所有零售户订货总量   上次同期
                    ring_order_sum = day_filter.select("sale_center_id", "qty_sum") \
                        .groupBy("sale_center_id") \
                        .agg(f.sum("qty_sum").alias("ring_qty_sum"))

                    # 本县或区所有零售户订货金额总和  上次同期
                    ring_amount_total = day_filter.select("sale_center_id", "amt_sum") \
                        .groupBy("sale_center_id") \
                        .agg(f.sum("amt_sum").alias("ring_amt_sum"))

                    # 本县或区所有零售户订货条均价   上次同期
                    ring_avg_price = ring_order_sum.join(ring_amount_total, "sale_center_id") \
                        .withColumn("ring_avg_price", divider_udf(col("ring_amt_sum"), col("ring_qty_sum")))


                    # 本县或区所有零售户订单数总和  上次同期
                    ring_order_count=day_filter.groupBy("sale_center_id") \
                        .count() \
                        .withColumnRenamed("count","ring_count")

                    ring_sum_colName = cols0["ring_sum"][i]
                    ring_amount_colName = cols0["ring_amount"][i]
                    ring_price_colName = cols0["ring_price"][i]
                    ring_order_colName = cols0["ring_orders"][i]


                    print(f"{str(dt.now())}  订货总量环比变化情况 {day}")
                    # 本县或区 上一周/上两周/上四周/上个月订单总量环比变化情况
                    try:
                        order_sum_ring_ratio = ring_order_sum.join(order_sum, "sale_center_id") \
                            .withColumn(ring_sum_colName, period_udf(col(sum_colName), col("ring_qty_sum")))
                        order_sum_ring_ratio.foreachPartition(lambda x: write_hbase1(x, [ring_sum_colName],hbase))
                    except Exception:
                        tb.print_exc()

                    print(f"{str(dt.now())}  订货总金额环比变化情况 {day}")
                    # 本县或区 上一周/上两周/上四周/上个月订货总金额环比变化情况
                    try:
                        amount_ring_ratio = ring_amount_total.join(amount_total, "sale_center_id") \
                            .withColumn(ring_amount_colName, period_udf(col(amount_colName), col("ring_amt_sum")))
                        amount_ring_ratio.foreachPartition(lambda x: write_hbase1(x, [ring_amount_colName],hbase))
                    except Exception:
                        tb.print_exc()

                    print(f"{str(dt.now())}  订货条均价环比变化情况 {day}")
                    # 本县或区 上一周/上两周/上四周/上个月订货条均价环比变化情况
                    try:
                        avg_price_ring_ratio = ring_avg_price.join(avg_price, "sale_center_id") \
                            .withColumn(ring_price_colName, period_udf(col(price_colName), col("ring_avg_price")))
                        avg_price_ring_ratio.foreachPartition(lambda x: write_hbase1(x, [ring_price_colName],hbase))
                    except Exception:
                        tb.print_exc()

                    print(f"{str(dt.now())}  订单数环比变化情况 {day}")
                    try:
                        order_count_ring_ratio = ring_order_count.join(order_count, "sale_center_id") \
                            .withColumn(ring_order_colName, period_udf(col(orders_colName), col("ring_count")))
                        order_count_ring_ratio.foreachPartition(lambda x: write_hbase1(x, [ring_order_colName],hbase))
                    except Exception:
                        tb.print_exc()
                except Exception:
                    tb.print_exc()
    except Exception:
        tb.print_exc()
get_order_stats_info()




def get_order_yoy_info():
    try:
        colNames=["qty_sum", "amt_sum", "sale_center_id", "born_date", "today"]
        co_co_01 = get_co_co_01(spark).select(colNames) \
            .withColumn("week_diff", week_diff("today", "born_date")) \
            .withColumn("month_diff", month_diff_udf(f.year(col("born_date")), f.month(col("born_date")), f.year(col("today")),f.month(col("today")))) \
            .where(col("month_diff") == 2)
        #-------------co_co_01  去年  同比情况
        last_year =get_co_co_01(spark).select(colNames) \
            .withColumn("last_year_today", f.date_sub(col("today"), 365)) \
            .withColumn("month_diff",month_diff_udf(f.year(col("born_date")), f.month(col("born_date")), f.year(col("last_year_today")),f.month(col("last_year_today")))) \
            .where(col("month_diff") == 1)



        print(f"{str(dt.now())}  本县或区所有零售户上个月订货总量同比变化情况")
        try:
            # 本县或区所有零售户上个月订货总量同比变化情况
            # 上个月
            last_month_qty_sum = co_co_01.where(col("month_diff") == 1) \
                .groupBy("sale_center_id") \
                .agg(f.sum("qty_sum").alias("qty_sum"))
            # 去年同期
            last_year_qty_sum = last_year.groupBy("sale_center_id") \
                .agg(f.sum("qty_sum").alias("last_year_qty_sum"))
            #订货总量同比
            colName="sum_lyear"
            last_year_qty_sum.join(last_month_qty_sum, "sale_center_id") \
                .withColumn(colName, period_udf(col("qty_sum"), col("last_year_qty_sum")))\
                .foreachPartition(lambda x:write_hbase1(x,[colName],hbase))
        except Exception:
            tb.print_exc()

        print(f"{str(dt.now())}  本县或区所有零售户上个月订货总金额同比变化情况")
        try:
            # 本县或区所有零售户上个月订货总金额同比变化情况
            # 上个月
            last_month_amt_sum = co_co_01.where(col("month_diff") == 1) \
                .groupBy("sale_center_id") \
                .agg(f.sum("amt_sum").alias("amt_sum"))
            # 去年同期
            last_year_amt_sum = last_year.groupBy("sale_center_id") \
                .agg(f.sum("amt_sum").alias("last_year_amt_sum"))
            #订货总金额同比
            colName="amount_lyear"
            last_year_amt_sum.join(last_month_amt_sum, "sale_center_id") \
                .withColumn(colName, period_udf(col("amt_sum"), col("last_year_amt_sum")))\
                .foreachPartition(lambda x:write_hbase1(x,[colName],hbase))
        except Exception:
             tb.print_exc()

        print(f"{str(dt.now())}  本县或区所有零售户上个月订货条均价同比变化情况")
        try:
            # 本县或区所有零售户上个月订货条均价同比变化情况
            # 上个月
            last_month_avg_price = last_month_qty_sum.join(last_month_amt_sum, "sale_center_id") \
                .withColumn("avg_price", divider_udf(col("amt_sum"), col("qty_sum")))
            # 去年同期
            last_year_avg_price = last_year_qty_sum.join(last_year_amt_sum, "sale_center_id") \
                .withColumn("last_year_avg_price", divider_udf(col("last_year_amt_sum"), col("last_year_qty_sum")))
            #订货均价同比
            colName="price_lyear"
            last_year_avg_price.join(last_month_avg_price, "sale_center_id") \
                .withColumn(colName, period_udf(col("avg_price"), col("last_year_avg_price")))\
                .foreachPartition(lambda x:write_hbase1(x,[colName],hbase))
        except Exception:
            tb.print_exc()

        print(f"{str(dt.now())}  本县或区所有零售户上个月订单数同比变化情况")
        try:
            #本县或区所有零售户上个月订单数同比变化情况
            # 上个月
            last_month_order_count = co_co_01.where(col("month_diff") == 1) \
                .groupBy("sale_center_id")\
                .count()
            # 去年同期
            last_year_order_count = last_year.groupBy("sale_center_id") \
                .count()\
                .withColumnRenamed("count","last_year_count")
            #订货总金额同比
            colName="orders_lyear"
            last_year_order_count.join(last_month_order_count, "sale_center_id") \
                .withColumn(colName, period_udf(col("count"), col("last_year_count")))\
                .foreachPartition(lambda x:write_hbase1(x,[colName],hbase))
        except Exception:
            tb.print_exc()
    except Exception:
        tb.print_exc()

get_order_yoy_info()



def get_ratio():
    try:
        # -------------------获取co_co_line
        colNames=["item_id","qty_ord","price","sale_center_id","born_date","today"]
        co_co_line=get_co_co_line(spark).select(colNames) \
            .withColumn("week_diff", week_diff("today", "born_date")) \
            .withColumn("month_diff",month_diff_udf(f.year(col("born_date")), f.month(col("born_date")), f.year(col("today")),f.month(col("today")))) \
            .where(col("month_diff") <= 12)
        # ------------------获取plm_item
        plm_item =get_plm_item(spark).select("item_id","yieldly_type","kind","item_name")
    
        line_plm = co_co_line.join(plm_item, "item_id")
    
    
        # 总量
        #上1周
        total_week_1 = co_co_line.where(col("week_diff") == 1) \
            .groupBy("sale_center_id") \
            .agg(f.sum("qty_ord").alias("qty_ord")).coalesce(10)
    
        filter_this_month=co_co_line.where(col("month_diff") > 0)
        #上个月
        total_month_1 = filter_this_month.where(col("month_diff") <= 1) \
            .groupBy("sale_center_id") \
            .agg(f.sum("qty_ord").alias("qty_ord")).coalesce(10)
        #上3个月
        total_month_3 = filter_this_month.where(col("month_diff") <= 3) \
            .groupBy("sale_center_id") \
            .agg(f.sum("qty_ord").alias("qty_ord")).coalesce(10)
        #上6个月
        total_month_6 = filter_this_month.where(col("month_diff") <= 6) \
            .groupBy("sale_center_id") \
            .agg(f.sum("qty_ord").alias("qty_ord")).coalesce(10)
        #上12个月
        total_month_12 = filter_this_month.where(col("month_diff") <= 12) \
            .groupBy("sale_center_id") \
            .agg(f.sum("qty_ord").alias("qty_ord")).coalesce(10)
    
    
        days = {"1_week":1, "1_month":1, "3_month":3, "6_month":6, "12_month":12}
        days_total = {"1_week": total_week_1, "1_month": total_month_1, "3_month": total_month_3, "6_month": total_month_6, "12_month": total_month_12}
    
        # 省内 0  省外 1 国外 3
        yieldly_types = ["0", "1", "3"]
        cols1 = {
            "1_week": ["in_prov_last_week","out_prov_last_week","import_last_week"],
            "1_month": ["in_prov_last_month","out_prov_last_month","import_last_month"],
            "3_month": ["in_prov_last_three_month","out_prov_last_three_month","import_last_three_month"],
            "6_month":["in_prov_last_half_year","out_prov_last_half_year","import_last_half_year"],
            "12_month":["in_prov_last_year","out_prov_last_year","import_last_year"]
        }
        # 省内 0 省外 1 的top5
        cols2 = {
            "1_week":["in_prov_top5_last_week","out_prov_top5_last_week"],
            "1_month":["in_prov_top5_last_month","out_prov_top5_last_month"],
            "3_month":["in_prov_top5_last_three_month","out_prov_top5_last_three_month"],
            "6_month":["in_prov_top5_last_half_year","out_prov_top5_last_half_year"],
            "12_month":["in_prov_top5_last_year","out_prov_top5_last_year"]
        }
        # 1:一类,2:二类,3:三类,4:四类,5:五类,6:无价类
        kinds=["1","2","3","4","5","6"]
        cols3={
            "1_week":["price_ratio_last_week_1","price_ratio_last_week_2","price_ratio_last_week_3","price_ratio_last_week_4","price_ratio_last_week_5","price_ratio_last_week_6"],
            "1_month":["price_ratio_last_month_1","price_ratio_last_month_2","price_ratio_last_month_3","price_ratio_last_month_4","price_ratio_last_month_5","price_ratio_last_month_6"],
            "3_month":["price_ratio_last_three_month_1","price_ratio_last_three_month_2","price_ratio_last_three_month_3","price_ratio_last_three_month_4","price_ratio_last_three_month_5","price_ratio_last_three_month_6"],
            "6_month":["price_ratio_last_half_year_1","price_ratio_last_half_year_2","price_ratio_last_half_year_3","price_ratio_last_half_year_4","price_ratio_last_half_year_5","price_ratio_last_half_year_6"],
            "12_month":["price_ratio_last_year_1","price_ratio_last_year_2","price_ratio_last_year_3","price_ratio_last_year_4","price_ratio_last_year_5","price_ratio_last_year_6"]
        }
    
        # 50以下；50（含）-100，100（含）-300，300（含）-500，500（含）以上
        prices = [0, 50, 100, 300, 500]
        cols4 = {
            "1_week": ["price_sub_last_week_under50", "price_sub_last_week_50_to_100", "price_sub_last_week_100_to_300","price_sub_last_week_300_to_500", "price_sub_last_week_up500"],
            "1_month": ["price_sub_last_month_under50", "price_sub_last_month_50_to_100", "price_sub_last_month_100_to_300","price_sub_last_month_300_to_500", "price_sub_last_month_up500"],
            "3_month": ["price_sub_last_three_month_under50", "price_sub_last_three_month_50_to_100","price_sub_last_three_month_100_to_300", "price_sub_last_three_month_300_to_500","price_sub_last_three_month_up500"],
            "6_month": ["price_sub_last_half_year_under50", "price_sub_last_half_year_50_to_100","price_sub_last_half_year_100_to_300", "price_sub_last_half_year_300_to_500","price_sub_last_half_year_up500"],
            "12_month": ["price_sub_last_year_under50", "price_sub_last_year_50_to_100", "price_sub_last_year_100_to_300","price_sub_last_year_300_to_500", "price_sub_last_year_up500"]
        }
    
    
        win = Window.partitionBy("sale_center_id").orderBy(f.desc("yieldly_type_qty_ord"))
    
        for key in days.keys():
            #对应日期的烟总量
            total_df=days_total[key]
            #日期过滤条件
            day = days[key]
            if key == "1_week":
                day_filter = line_plm.where(col("week_diff") == day)
            else:
                day_filter = line_plm.where((col("month_diff") >0) & (col("month_diff") <= day))

            # 本县或区所有零售户上1周，上个月，上3个月，上6个月，上12个月所订省内烟、省外烟、进口烟的占比（省内外烟）
            for i in range(len(yieldly_types)):
                yieldly_type = yieldly_types[i]
                yieldly_type_filter = day_filter.where(col("yieldly_type") == yieldly_type)
                colName = cols1[key][i]
                try:
                    print(f"{str(dt.now())}   yieldly_type:{yieldly_type} {key}:{day}")
                    yieldly_type_filter.groupBy("sale_center_id") \
                        .agg(f.sum("qty_ord").alias("yieldly_type_qty_ord")) \
                        .join(total_df, "sale_center_id") \
                        .withColumn(colName, divider_udf(col("yieldly_type_qty_ord"), col("qty_ord"))) \
                        .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))
                except Exception:
                    tb.print_exc()


                # top 5
                # 本县或区所有零售户上1周，上个月，上3个月，上6个月，上12个月订购数前5省内烟/省外
                if yieldly_type in ["0", "1"]:
                    print(f"{str(dt.now())}   top5   yieldly_type:{yieldly_type} {key}:{day}")
                    try:
                        colName = cols2[key][i]

                        top5=yieldly_type_filter.coalesce(5).groupBy("sale_center_id", "item_id") \
                            .agg(f.sum("qty_ord").alias("yieldly_type_qty_ord")) \
                            .withColumn("rank", f.row_number().over(win)) \
                            .where(col("rank") <= 5)
                        top5.join(plm_item,"item_id")\
                            .groupBy("sale_center_id") \
                            .agg(f.collect_list("item_name").alias(colName)) \
                            .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))
                    except Exception:
                        tb.print_exc()

            # 本县或区所有零售户上1周，上个月，上3个月，上6个月，上12个月所订卷烟价类占比
            for i in range(len(kinds)):
                try:
                    kind=kinds[i]
                    kind_filter = day_filter.where(col("kind") == kind)
                    colName = cols3[key][i]
                    print(f"{str(dt.now())}   kind:{kind} {key}:{day}")

                    kind_filter.groupBy("sale_center_id") \
                        .agg(f.sum("qty_ord").alias("kind_qty_ord")) \
                        .join(total_df, "sale_center_id") \
                        .withColumn(colName, divider_udf(col("kind_qty_ord"), col("qty_ord")))\
                        .foreachPartition(lambda x: write_hbase1(x, [colName], hbase))
                except Exception:
                    tb.print_exc()


            # 本县或区所有零售户上1周，上个月，上3个月，上6个月，上12个月所订卷烟价格分段占比
            for i in range(len(prices)):
                price = prices[i]
                colName = cols4[key][i]
                if price == 500:
                    price_filter = day_filter.where(col("price") >= price)
                    print(f"{str(dt.now())}  price:({price},∞],{key}:{day}")
                else:
                    price_filter = day_filter.where((col("price") >= price) & (col("price") < prices[i + 1]))
                    print(f"{str(dt.now())}  price:({price},{prices[i+1]}],{key}:{day}")
                try:
                    price_filter.groupBy("sale_center_id") \
                        .agg(f.sum("qty_ord").alias("price_qty_ord")) \
                        .join(total_df, "sale_center_id", "left") \
                        .withColumn(colName, divider_udf(col("price_qty_ord"), col("qty_ord"))) \
                        .foreachPartition(lambda x: write_hbase1(x, [colName], hbase))
                except Exception:
                    tb.print_exc()
    except Exception:
        tb.print_exc()
get_ratio()





# 商圈指数（餐饮+娱乐+购物地点数）
# 交通（地铁站，公交站，机场，港口码头数量）指数

#餐厅、交通、商城、娱乐场馆等经纬度
def get_trans_busiArea_index():
    try:
        coordinate=get_coordinate(spark).select("lng","lat","types","adcode","cityname")

        area_code=get_area(spark).withColumnRenamed("区域编码","adcode")\
                        .select("adcode","county","sale_center_id")


        types={"trans_index":"(.*地铁站.*)|(.*公交.*)|(.*机场.*)|(.*港口码头.*)",
               "area_index":"(.*餐厅.*)|(.*咖啡厅.*)|(.*茶艺馆.*)|(.*甜品店.*)|(.*购物中心.*)|(.*运动场馆.*)|(.*娱乐场所.*)|(.*休闲场所.*)",
              }

        area_coor=coordinate.join(area_code,"adcode")

        for colName in types.keys():
            print(f"{str(dt.now())}   {colName}")
            regex=types[colName]
            try:
                #area_coor 先获取符合条件的服务
                count_df=area_coor.where(col("types").rlike(regex)) \
                    .groupBy("cityname", "sale_center_id") \
                    .count()
                #每个市的最大值
                max_df = count_df.groupBy("cityname").agg(f.max("count").alias("max"))

                count_df.join(max_df, "cityname")\
                    .withColumn(colName,f.round(divider_udf(col("count"), col("max"))*5,4)) \
                    .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))
            except Exception:
                tb.print_exc()
    except Exception:
        tb.print_exc()

get_trans_busiArea_index()




# 城市 city   县或区  county
def get_city():
    try:
        area_code=get_area(spark).select("city","county","sale_center_id")\
                  .groupBy("city","sale_center_id").agg(f.collect_list("county").alias("county"))\
                  .foreachPartition(lambda x:write_hbase1(x,["city","county"],hbase))
    except Exception:
        tb.print_exc()

get_city()





#GDP指数     需要区县名称与sale_center_id匹配关系0410+区域编码.csv
def get_gdp():
    try:
        area_code = get_area(spark).select("sale_center_id", "county")
        gdp_sum = get_city_info(spark).select("city", "county", "gdp", "mtime") \
            .where(col("mtime") == f"{dt.now().year-1}-12-01") \
            .join(area_code, "county") \
            .groupBy("city", "sale_center_id") \
            .agg(f.sum("gdp"))

        max_df = gdp_sum.groupBy("city").agg(f.max("sum(gdp)").alias("max"))
        colName="GDP"
        gdp_sum.join(max_df, "city") \
            .withColumn(colName, divider_udf(col("sum(gdp)"), col("max")) * 5) \
            .foreachPartition(lambda x: write_hbase1(x, [colName], hbase))

    except Exception:
        tb.print_exc()

get_gdp()





#人口指数
#区域竞争指数（人口数目/零售户数目）指数
def get_ppl_areaCpt_index():
    population = get_city_ppl(spark).select("city", "区县", "总人口")
    area_code = get_area(spark).where(col("city").rlike("株洲市|邵阳市|岳阳市")) \
        .select("county", "sale_center_id")
    area_plt = population.join(area_code, col("county") == col("区县"))

    # 每个区域中心的人口
    sale_center_ppl = area_plt.groupBy("city", "sale_center_id") \
        .agg(f.sum("总人口").alias("ppl"))

    # -----人口指数
    print(f"{str(dt.now())}  人口指数")
    try:
        colName = "population_index"
        # 每个市人口最多的区域中心的人口
        max_df = sale_center_ppl.groupBy("city") \
            .agg(f.max("ppl").alias("max"))

        sale_center_ppl.join(max_df, "city") \
            .withColumn(colName, divider_udf(col("ppl"), col("max")) * 5) \
            .foreachPartition(lambda x: write_hbase1(x, [colName], hbase))
    except Exception:
        tb.print_exc()


    #-----区域竞争力指数
    try:
        # 区域零售户数目
        cust_num = get_co_cust(spark).select("sale_center_id") \
            .groupBy("sale_center_id").count()

        # 人口数目/零售户数目
        print(f"{str(dt.now())}  区域竞争指数（人口数目/零售户数目）指数")
        ppl_cust = sale_center_ppl.join(cust_num, "sale_center_id") \
            .withColumn("ppl_cust", divider_udf(col("ppl"), col("count")))

        max_df = ppl_cust.groupBy("city").agg(f.max("ppl_cust").alias("max"))
        colName = "area_competitive_index"
        ppl_cust.join(max_df, "city") \
            .withColumn(colName, divider_udf(col("ppl_cust"), col("max")) * 5) \
            .select("sale_center_id", colName)\
            .foreachPartition(lambda x:write_hbase1(x,[colName],hbase))
    except Exception:
        tb.print_exc()

get_ppl_areaCpt_index()
