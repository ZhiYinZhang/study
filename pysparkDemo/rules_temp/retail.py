#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/11 14:59
import traceback as tb
from pyspark.sql.functions import col
from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from pyspark import SparkContext
from datetime import datetime as dt
from pyspark.sql import Window
from pysparkDemo.rules_temp.write_hbase import write_hbase1
from pysparkDemo.rules_temp.utils import *

spark = SparkSession.builder\
                    .enableHiveSupport()\
                    .appName("retail")\
                    .getOrCreate()
sc=spark.sparkContext
sc.setLogLevel("WARN")

spark.sql("use aistrong")

hbase={"table":"TOBACCO.RETAIL","families":["0"],"row":"cust_id"}
hbase={"table":"test_ma","families":["info"],"row":"cust_id"}


# --------------------------------------------------------retail（零售户信息）基本信息---------------------------------------------
def get_cust_info():
    co_cust_cols=["cust_id","cust_name","cust_seg","status","pay_type","license_code","manager","identity_card_id",
                  "order_tel","inv_type","order_way","periods","busi_addr","work_port","base_type","sale_scope",
                  "scope","com_chara","is_tor_tax","is_sale_large","is_rail_cust","area_type","is_sefl_cust","is_func_cust",
                  "dt"]
    # 需要修改的列名   与hbase的列名对应
    co_cust_renamed = {"is_tor_tax": "tor_tax", "is_sale_large": "sale_large",
                       "is_rail_cust": "rail_cust", "is_sefl_cust": "sefl_cust",
                       "is_func_cust": "func_cust", "cust_seg": "grade"
                       }
    crm_cust_cols=["cust_id","longitude","latitude","is_multiple_shop","org_model","is_night_shop",
                   "busi_time_type","consumer_group","consumer_attr","busi_type","compliance_grade"]
    crm_cust_renamed = {"is_multiple_shop": "multiple_shop", "is_night_shop": "night_shop"}

    print(f"{str(dt.now())} co_cust/crm_cust零售户信息")
    try:
        co_cust = get_co_cust(spark).select(co_cust_cols)
        for key in co_cust_renamed.keys():
            co_cust = co_cust.withColumnRenamed(key, co_cust_renamed[key])


        crm_cust=get_crm_cust(spark).select(crm_cust_cols)
        for key in crm_cust_renamed.keys():
            crm_cust = crm_cust.withColumnRenamed(key, crm_cust_renamed[key])


        # cols = co_cust.columns
        # co_cust.foreachPartition(lambda x: write_hbase1(x, cols,hbase))
        return crm_cust.join(co_cust, "cust_id")
    except Exception:
        tb.print_exc()

cust_info = get_cust_info()
cols = cust_info.columns
cust_info.foreachPartition(lambda x: write_hbase1(x, cols,hbase))


def get_acc():
    #acc是扣款账号  不是co_cust里面的开户银行账号
    print(f"{str(dt.now())} 扣款账号")
    try:
        co_cust = get_co_cust(spark).select("cust_id")
        get_co_debit_acc(spark).select("cust_id", "acc") \
            .join(co_cust, "cust_id") \
            .foreachPartition(lambda x: write_hbase1(x, ["acc"], hbase))
    except Exception:
        tb.print_exc()

get_acc()



def get_city_county():
    #-----市 区 sale_center_id
    print(f"{str(dt.now())} city county abcode")
    try:
        co_cust=get_co_cust(spark).select("cust_id","com_id","sale_center_id")

        area_code=get_area(spark)

        co_cust.join(area_code,["com_id","sale_center_id"])\
               .withColumnRenamed("城市","city")\
               .withColumnRenamed("区","county")\
               .withColumnRenamed("sale_center_id","abcode")\
               .foreachPartition(lambda x: write_hbase1(x, ["abcode","city","county"],hbase))
    except Exception:
        tb.print_exc()

get_city_county()





def get_lng_lat():
    #-----网上爬取的经纬度
    print(f"{str(dt.now())}   经纬度")
    try:
        co_cust=get_co_cust(spark).select("cust_id")

        #每个城市的零售户的经纬度
        cust_lng_lat=get_cust_lng_lat(spark).withColumn("cust_id",fill_0_udf(col("cust_id")))

        lng_lat=cust_lng_lat.select("cust_id","longitude","latitude")\
                           .join(co_cust,"cust_id")
        return lng_lat
    except Exception:
       tb.print_exc()

lng_lat=get_lng_lat()
lng_lat.foreachPartition(lambda x:write_hbase1(x,["longitude","latitude"],hbase))





def get_30_grade_change():
    crm_cust_log=get_crm_cust_log(spark).where(col("day_diff") <= 30)
    try:
        # 档位变更
        cust_seg = crm_cust_log.where(col("change_type") == "CO_CUST.CUST_SEG")

        #-----前30天档位变更次数
        print(f"{str(dt.now())}   前30天档位变更次数")
        colName="grade_change_count"
        cust_seg.groupBy("cust_id") \
            .agg(f.count("change_type").alias(colName)) \
            .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))


        #-----前30天档位变更差
        print(f"{str(dt.now())}   前30天档位变更差")
        colName="grade_abs"
        cust_seg.groupBy("cust_id") \
            .agg(grade_diff_udf(f.max("change_frm"), f.min("change_frm"), f.max("change_to"), f.min("change_to")).alias(colName)) \
            .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))

        #-----30天前档位
        print(f"{str(dt.now())}   30天前档位")
        """
        这里只计算了近30天改变了档位的用户，如果用户近30天都没有改变过档位，这里是没有数据的，
        没有数据的在前端判断，如果这个指标为空，设为现时档位(前端在展示时，这两个是一起读的，所以更方便)
        """
        colName="grade_before"
        cust_seg.groupBy("cust_id") \
            .agg(f.min("audit_date").alias("audit_date")) \
            .join(cust_seg, ["cust_id", "audit_date"]) \
            .withColumnRenamed("change_frm", colName) \
            .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))
    except Exception:
        tb.print_exc()

    try:
        # 状态变更
        status = crm_cust_log.where(col("change_type") == "CO_CUST.STATUS")

        #-----前30天状态变更次数
        print(f"{str(dt.now())}   前30天状态变更次数")
        colName="status_change_count"
        status.groupBy("cust_id") \
            .agg(f.count("change_type").alias(colName)) \
            .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))
        #-----前30天状态
        print(f"{str(dt.now())}   前30天状态")
        """
        这里只计算了近30天改变了状态的用户，如果用户近30天都没有改变过状态，这里是没有数据的，
        没有数据的在前端判断，如果这个指标为空，设为现时状态(前端在展示时，这两个是一起读的，所以更方便)
        """
        colName="status_before"
        status.groupBy("cust_id") \
            .agg(f.min("audit_date").alias("audit_date")) \
            .join(status, ["cust_id", "audit_date"]) \
            .select("cust_id", "change_frm") \
            .withColumnRenamed("change_frm", colName) \
            .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))
    except Exception:
        tb.print_exc()

get_30_grade_change()





#-----是否存在实际经营人与持证人不符
def is_match(x, y):
    if x == y:
        result = "1"
    else:
        result = "0"
    return result
isMatch = card_pass = f.udf(is_match)
def card_passId():
    try:
        co_cust=get_co_cust(spark).select("cust_id","identity_card_id")

        co_debit_acc=get_co_debit_acc(spark).select("cust_id","pass_id")

        colName = "license_not_match"
        result=co_cust.join(co_debit_acc, "cust_id") \
                   .withColumn(colName, isMatch(col("identity_card_id"), col("pass_id")))
        return result
    except Exception:
        tb.print_exc()


card_passId().foreachPartition(lambda x: write_hbase1(x, ["license_not_match"],hbase))





# 是否隐形连锁户??????????





# ------------------------------------------  retail（零售户分析）表 统计类信息--------------------------------

def get_order_stats_info():
    co_co_01=get_co_co_01(spark).select("cust_id","qty_sum","amt_sum","today","born_date")\
                  .withColumn("week_diff", week_diff("today","born_date"))\
                  .withColumn("month_diff",month_diff_udf(f.year(col("born_date")),f.month(col("born_date")),f.year(col("today")),f.month(col("today"))))\
                  .where(col("month_diff") <= 2)

    #上一周/上两周/上四周/上个月
    days = [1, 2, 4, 1]
    cols0 = {
        "sum": ["sum_last_week", "sum_last_two_week", "sum_last_four_week", "sum_last_month"],
        "amount": ["amount_last_week", "amount_last_two_week", "amount_last_four_week", "amount_last_month"],
        "price": ["price_last_week", "price_last_two_week", "price_last_four_week", "price_last_month"],
        "orders": ["orders_last_week", "orders_last_two_week", "orders_last_four_week", "orders_last_month"],

        "ring_sum": ["sum_ring_last_week", "sum_ring_last_two_week", "sum_ring_last_four_week", "sum_ring_last_month"],
        "ring_amount": ["amount_ring_last_week", "amount_ring_last_two_week", "amount_ring_last_four_week","amount_ring_last_month"],
        "ring_price": ["price_ring_last_week", "price_ring_last_two_week", "price_ring_last_four_week","price_ring_last_month"]
        }
    for i in range(len(days)):
        try:
            day = days[i]
            sum_colName = cols0["sum"][i]
            amount_colName = cols0["amount"][i]
            price_colName = cols0["price"][i]
            orders_colName = cols0["orders"][i]

            if i < 3:
                day_filter = co_co_01.where((col("week_diff") > 0) & (col("week_diff") <= day))
            else:
                day_filter = co_co_01.where(col("month_diff") == day)

            #-----某零售户上一周/上两周/上四周/上个月订货总量
            print(f"{str(dt.now())}  {day} 订货总量")
            try:
                order_total = day_filter \
                    .groupBy("cust_id") \
                    .agg(f.sum("qty_sum").alias(sum_colName))
                order_total.foreachPartition(lambda x: write_hbase1(x, [sum_colName],hbase))
            except Exception:
                tb.print_exc()

            print(f"{str(dt.now())}  {day} 订货金额")
            #-----某零售户上一周/上两周/上四周/上个月的订货金额
            try:
                amount_total = day_filter \
                    .groupBy("cust_id") \
                    .agg(f.sum("amt_sum").alias(amount_colName))
                amount_total.foreachPartition(lambda x: write_hbase1(x, [amount_colName],hbase))
            except Exception:
                tb.print_exc()
                
            print(f"{str(dt.now())}  {day} 订货条均价")
            #-----某零售户上一周/上两周/上四周/上个月的订货条均价
            try:
                avg_price = order_total.join(amount_total, "cust_id") \
                    .withColumn(price_colName, divider_udf(col(amount_colName), col(sum_colName)))
                avg_price.foreachPartition(lambda x: write_hbase1(x, [price_colName],hbase))
            except Exception:
                tb.print_exc()
                
            print(f"{str(dt.now())}  {day} 订单数")
            #-----某零售户上一周/上两周/上四周/上个月的订单数
            try:
                day_filter.groupBy("cust_id") \
                    .count() \
                    .withColumnRenamed("count", orders_colName) \
                    .foreachPartition(lambda x: write_hbase1(x, [orders_colName],hbase))
            except Exception:
                tb.print_exc()


            # -------------------------------------环比-----------------------------------
            # (1,2] (1,3] (1,5] (1,2]
            if i < 3:
                day_filter = co_co_01.where((col("week_diff") > 1) & (col("week_diff") <= (1 + day)))
            else:
                day_filter = co_co_01.where((col("month_diff") > 1) & (col("month_diff") <= (1 + day)))

            # 某零售户上次同期   订货总量
            ring_order_total = day_filter \
                .groupBy("cust_id") \
                .agg(f.sum("qty_sum").alias("ring_qty_ord"))
            # 某零售户上次同期   订货总金额
            ring_amount_total = day_filter \
                .groupBy("cust_id") \
                .agg(f.sum("amt_sum").alias("ring_amt_sum"))
            # 某零售户上次同期   订货均价
            ring_avg_price = ring_order_total.join(ring_amount_total, "cust_id") \
                .withColumn("ring_avg_price", divider_udf(col("ring_amt_sum"), col("ring_qty_ord")))


            ring_sum_colName = cols0["ring_sum"][i]
            ring_amount_colName = cols0["ring_amount"][i]
            ring_price_colName = cols0["ring_price"][i]

            print(f"{str(dt.now())}  {day} 订货总量环比变化情况")
            #-----某零售户上一周/上两周/上四周/上个月订货总量环比变化情况
            try:
                order_ring_ratio = ring_order_total.join(order_total, "cust_id") \
                    .withColumn(ring_sum_colName, period_udf(col(sum_colName), col("ring_qty_ord")))
                order_ring_ratio.foreachPartition(lambda x: write_hbase1(x, [ring_sum_colName],hbase))
            except Exception:
                tb.print_exc()
                
            print(f"{str(dt.now())}  {day} 订货总金额环比变化情况")
            #-----某零售户上一周/上两周/上四周/上个月订货总金额环比变化情况
            try:
                amount_ring_ratio = ring_amount_total.join(amount_total, "cust_id") \
                    .withColumn(ring_amount_colName, period_udf(col(amount_colName), col("ring_amt_sum")))
                amount_ring_ratio.foreachPartition(lambda x: write_hbase1(x, [ring_amount_colName],hbase))
            except Exception:
                tb.print_exc()
                
            print(f"{str(dt.now())}  {day} 订货条均价环比变化情况")
            #-----某零售户上一周/上两周/上四周/上个月订货条均价环比变化情况
            try:
                avg_price_ring_ratio = ring_avg_price.join(avg_price, "cust_id") \
                    .withColumn(ring_price_colName, period_udf(col(price_colName), col("ring_avg_price")))
                avg_price_ring_ratio.foreachPartition(lambda x: write_hbase1(x, [ring_price_colName],hbase))
            except Exception:
                tb.print_exc()
        except Exception:
            tb.print_exc()

get_order_stats_info()






def get_order_yoy_info():
    colNames=["cust_id", "qty_sum", "amt_sum", "today", "born_date"]
    co_co_01 = get_co_co_01(spark).select(colNames) \
                            .withColumn("week_diff", week_diff("today", "born_date")) \
                            .withColumn("month_diff",month_diff_udf(f.year(col("born_date")), f.month(col("born_date")), f.year(col("today")),f.month(col("today")))) \
                            .where(col("month_diff") == 1)
    #-------------去年
    last_year=get_co_co_01(spark).select(colNames) \
            .withColumn("last_year_today", f.date_sub(col("today"), 365)) \
            .withColumn("month_diff",month_diff_udf(f.year(col("born_date")), f.month(col("born_date")), f.year(col("last_year_today")),f.month(col("last_year_today")))) \
            .where(col("month_diff") == 1)
    try:
        #-----某零售户上个月订货总量同比变化情况
        try:
            # 上个月
            last_month_qty_sum = co_co_01.groupBy("cust_id") \
                .agg(f.sum("qty_sum").alias("qty_sum"))
            # 去年同期
            last_year_qty_sum = last_year.groupBy("cust_id") \
                .agg(f.sum("qty_sum").alias("last_year_qty_sum"))
    
            colName="sum_lyear"
            print(f"{str(dt.now())}  某零售户上个月订货总量同比变化情况  ",colName)
            last_year_qty_sum.join(last_month_qty_sum, "cust_id") \
                .withColumn(colName, period_udf(col("qty_sum"), col("last_year_qty_sum")))\
                .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))
        except Exception:
            tb.print_exc()

        #-----某零售户上个月订货总金额同比变化情况
        try:
            # 上个月
            last_month_amt_sum = co_co_01.groupBy("cust_id") \
                .agg(f.sum("amt_sum").alias("amt_sum"))
            # 去年同期
            last_year_amt_sum = last_year.groupBy("cust_id") \
                .agg(f.sum("amt_sum").alias("last_year_amt_sum"))
            colName="amount_lyear"
            print(f"{str(dt.now())}  某零售户上个月订货总金额同比变化情况  ", colName)
            last_year_amt_sum.join(last_month_amt_sum, "cust_id") \
                .withColumn(colName, period_udf(col("amt_sum"), col("last_year_amt_sum")))\
                .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))
        except Exception:
            tb.print_exc()

        #-----某零售户上个月订货条均价同比变化情况
        try:
            # 上个月
            last_month_avg_price = last_month_qty_sum.join(last_month_amt_sum, "cust_id") \
                .withColumn("avg_price", divider_udf(col("amt_sum"), col("qty_sum")))
            # 去年同期
            last_year_avg_price = last_year_qty_sum.join(last_year_amt_sum, "cust_id") \
                .withColumn("last_year_avg_price", divider_udf(col("last_year_amt_sum"), col("last_year_qty_sum")))
            colName="price_lyear"
            print(f"{str(dt.now())}  某零售户上个月订货条均价同比变化情况  ", colName)
            last_year_avg_price.join(last_month_avg_price, "cust_id") \
                .withColumn(colName, period_udf(col("avg_price"), col("last_year_avg_price")))\
                .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))
        except Exception:
            tb.print_exc()
    except Exception:
        print(colName)
        tb.print_exc()

get_order_yoy_info()





def get_ratio():
    try:
        co_co_line=get_co_co_line(spark).select("cust_id","item_id","qty_ord","price","born_date","today") \
                        .withColumn("week_diff", week_diff("today", "born_date")) \
                        .withColumn("month_diff",month_diff_udf(f.year(col("born_date")), f.month(col("born_date")), f.year(col("today")),f.month(col("today")))) \
                        .where(col("month_diff") <= 12)
    
        plm_item = get_plm_item(spark).select("item_id","yieldly_type","kind","item_name")
    
        line_plm = co_co_line.join(plm_item, "item_id")
    
    
        # 总量
        #上1周
        total_week_1 = co_co_line.where(col("week_diff") == 1) \
            .groupBy("cust_id") \
            .agg(f.sum("qty_ord").alias("qty_ord")).coalesce(10)
    
        filter_this_month=co_co_line.where(col("month_diff") > 0)
        #上个月
        total_month_1 = filter_this_month.where(col("month_diff") <= 1) \
            .groupBy("cust_id") \
            .agg(f.sum("qty_ord").alias("qty_ord")).coalesce(10)
        #上3个月
        total_month_3 = filter_this_month.where(col("month_diff") <= 3) \
            .groupBy("cust_id") \
            .agg(f.sum("qty_ord").alias("qty_ord")).coalesce(10)
        #上6个月
        total_month_6 = filter_this_month.where(col("month_diff") <= 6) \
            .groupBy("cust_id") \
            .agg(f.sum("qty_ord").alias("qty_ord")).coalesce(10)
        #上12个月
        total_month_12 = filter_this_month.where(col("month_diff") <= 12) \
            .groupBy("cust_id") \
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
        prices =[0,50,100,300,500]
        cols4={
            "1_week":["price_sub_last_week_under50","price_sub_last_week_50_to_100","price_sub_last_week_100_to_300","price_sub_last_week_300_to_500","price_sub_last_week_up500"],
            "1_month":["price_sub_last_month_under50","price_sub_last_month_50_to_100","price_sub_last_month_100_to_300","price_sub_last_month_300_to_500","price_sub_last_month_up500"],
            "3_month":["price_sub_last_three_month_under50","price_sub_last_three_month_50_to_100","price_sub_last_three_month_100_to_300","price_sub_last_three_month_300_to_500","price_sub_last_three_month_up500"],
            "6_month":["price_sub_last_half_year_under50","price_sub_last_half_year_50_to_100","price_sub_last_half_year_100_to_300","price_sub_last_half_year_300_to_500","price_sub_last_half_year_up500"],
            "12_month":["price_sub_last_year_under50","price_sub_last_year_50_to_100","price_sub_last_year_100_to_300","price_sub_last_year_300_to_500","price_sub_last_year_up500"]
        }
    
    
    
        win = Window.partitionBy("cust_id").orderBy(f.desc("yieldly_type_qty_ord"))
    
        for key in days.keys():
            #对应日期的烟总量
            total_df=days_total[key]
            #日期过滤条件
            day = days[key]
            if key == "1_week":
                day_filter = line_plm.where(col("week_diff") == day)
            else:
                day_filter = line_plm.where((col("month_diff") >0) & (col("month_diff") <= day))
    
            #-----某零售户上1周，上个月，上3个月，上6个月，上12个月所订省内烟、省外烟、进口烟的占比（省内外烟）
            for i in range(len(yieldly_types)):
                yieldly_type = yieldly_types[i]
                yieldly_type_filter = day_filter.where(col("yieldly_type") == yieldly_type)
    
                try:
                    colName = cols1[key][i]
                    print(f"{str(dt.now())}  yieldly_type:{yieldly_type} {key}:{day}")
                    yieldly_type_filter.groupBy("cust_id") \
                        .agg(f.sum("qty_ord").alias("yieldly_type_qty_ord")) \
                        .join(total_df, "cust_id") \
                        .withColumn(colName, divider_udf(col("yieldly_type_qty_ord"), col("qty_ord"))) \
                        .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))
                except Exception:
                        print(f"error   yieldly_type:{yieldly_type} {key}:{day}")
                        tb.print_exc()
    
                #-----零售户上1周，上个月，上3个月，上6个月，上12个月订购数前5省内烟
                #-----零售户上1周，上个月，上3个月，上6个月，上12个月订购数前5省外烟
                
                if yieldly_type in ["0", "1"]:
                    try:
                        print(f"{str(dt.now())}  top5   yieldly_type:{yieldly_type} {key}:{day}")
                        colName = cols2[key][i]
                        top5=yieldly_type_filter.groupBy("cust_id", "item_id") \
                            .agg(f.sum("qty_ord").alias("yieldly_type_qty_ord")) \
                            .withColumn("rank", f.row_number().over(win)) \
                            .where(col("rank") <= 5)
                        top5.join(plm_item,"item_id")\
                            .groupBy("cust_id") \
                            .agg(f.collect_list("item_name").alias(colName)) \
                            .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))
                    except Exception:
                        tb.print_exc()
    
            #-----某零售户上1周，上个月，上3个月，上6个月，上12个月所订卷烟价类占比
            for i in range(len(kinds)):
                kind=kinds[i]
                kind_filter = day_filter.where(col("kind") == kind)
                colName = cols3[key][i]
                print(f"{str(dt.now())}  kind:{kind} {key}:{day}")
                try:
                    kind_filter.groupBy("cust_id") \
                        .agg(f.sum("qty_ord").alias("kind_qty_ord")) \
                        .join(total_df, "cust_id") \
                        .withColumn(colName, divider_udf(col("kind_qty_ord"), col("qty_ord")))\
                        .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))
    
                except Exception:
                    print(f"error   kind:{kind},day:{day}")
                    tb.print_exc()
    
            # -----某零售户上1周，上个月，上3个月，上6个月，上12个月所订卷烟价格分段占比
            for i in range(len(prices)):
                try:
                    price=prices[i]
                    colName = cols4[key][i]
                    if price ==500:
                        price_filter = day_filter.where(col("price") >= price)
                        print(f"{str(dt.now())}  price:({price},∞],{key}:{day}")
                    else:
                        price_filter = day_filter.where((col("price") >= price) & (col("price") < prices[i + 1]))
                        print(f"{str(dt.now())}  price:({price},{prices[i+1]}],{key}:{day}")
    
                    price_filter.groupBy("cust_id") \
                            .agg(f.sum("qty_ord").alias("price_qty_ord")) \
                            .join(total_df, "cust_id", "left") \
                            .withColumn(colName, divider_udf(col("price_qty_ord"), col("qty_ord"))) \
                            .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))
                except Exception:
                    tb.print_exc()
    except Exception:
        tb.print_exc()
get_ratio()






#零售户周边餐饮消费指数（餐厅，咖啡厅，茶艺馆，甜点店等的数量）
#零售户周边交通便捷指数（地铁站，公交车站，机场，港口码头数量）
#零售户周边购物消费指数（购物中心，普通商场，免税品店，市场，特色商业街，步行街，农贸市场数量）
#零售户周边娱乐休闲指数（运动场馆，娱乐场所，休闲场所数量）

"""
计算方式：零售户所在位置一定范围内（1000m）对应店铺数量/全市所有店铺同等计算方式的最大值*5
"""
def get_cust_index():
    # 餐厅、交通、商城、娱乐场馆等经纬度
    coordinate = get_coordinate(spark).select("lng", "lat", "types")

    types = {"catering_cons_count": "(.*餐厅.*)|(.*咖啡厅.*)|(.*茶艺馆.*)|(.*甜品.*)",
             "convenient_trans_count": "(.*地铁站.*)|(.*公交.*)|(.*机场.*)|(.*港口码头.*)",
             "shopping_cons_count": "(.*购物中心.*)|(.*普通商场.*)|(.*免税品店.*)|(.*市场.*)|(.*特色商业街.*)|(.*步行街.*)|(.*农贸市场.*)",
             "entertainment_count": "(.*运动场馆.*)|(.*娱乐场所.*)|(.*休闲场所.*)"}

    try:
        # 零售户经纬度
        co_cust=get_co_cust(spark).select("cust_id")
        cust_lng_lat = get_cust_lng_lat(spark).select("cust_id", "city", "longitude", "latitude") \
                                 .withColumn("cust_id", fill_0_udf(col("cust_id"))) \
                                 .join(co_cust, "cust_id")
        # 每个零售户  一公里的经度范围和纬度范围
        cust_lng_lat0 = cust_lng_lat.withColumn("scope",f.lit(1))\
            .withColumn("lng_l", lng_l(col("longitude"), col("latitude"),col("scope"))) \
            .withColumn("lng_r", lng_r(col("longitude"), col("latitude"),col("scope"))) \
            .withColumn("lat_d", lat_d(col("latitude"),col("scope"))) \
            .withColumn("lat_u", lat_u(col("latitude"),col("scope")))

        for y in range(len(types)):
            try:
                colName = list(types.keys())[y]
                regex = types[colName]

                print(f"{str(dt.now())}  {colName}指数")
                # coordinate先过滤符合条件的服务 再去join零售户
                count_df = coordinate.where(col("types").rlike(regex)) \
                    .join(cust_lng_lat0,(col("lng") >= col("lng_l")) & (col("lng") <= col("lng_r")) & (col("lat") >= col("lat_d")) & (col("lat") <= col("lat_u"))) \
                    .groupBy("city", "cust_id").count()

                # 全市所有店铺同等计算方式的最大值
                count_max = count_df.groupBy("city").agg(f.max("count").alias("max"))

                #result
                count_df.join(count_max, "city") \
                    .withColumn(colName, divider_udf(col("count"), col("max")) * 5) \
                    .foreachPartition(lambda x: write_hbase1(x, [colName], hbase))
            except Exception:
                tb.print_exc()
    except Exception:
        tb.print_exc()


get_cust_index()





#-----零售户周边消费水平指数
"""
零售户所在位置一定范围内（1000m）对应消费水平/全市所有店铺同等计算方式的最大值*5，
对应消费水平= 所在位置一定范围内（1000m）的（每平方米租金价格均值+餐饮价格均值+酒店价格均值）     
注：1）如果三项同时缺失，按缺失值展示 
2）如有其中一项缺失，按该项所有值从小到大排序，取25%分位处值填充
"""
def get_consume_level_index():
    try:
        consume_level_df=get_consume_level(spark)

        max_df = consume_level_df.groupBy("city").agg(f.max("consume_level").alias("max"))

        colName = "catering_cons_avg"
        print(f"{str(dt.now())}   consume level index")
        consume_level_df.join(max_df, "city") \
            .withColumn(colName, divider_udf(col("consume_level"), col("max")) * 5) \
            .foreachPartition(lambda x: write_hbase1(x, [colName], hbase))
    except Exception:
        tb.print_exc()


get_consume_level_index()