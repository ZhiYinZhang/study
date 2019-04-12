#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/11 14:59
from pyspark.sql.functions import col,to_date,current_date,datediff,udf,desc,row_number,collect_list,date_sub,count,year,month,sum
from pyspark.sql import SparkSession
from pyspark import SparkContext
from datetime import datetime as dt
from pyspark.sql import Window
from pysparkDemo.rules.write_hbase import hbase,write_hbase1
from pysparkDemo.rules.utils import divider_udf,period_udf,week_diff,month_diff_udf
spark = SparkSession.builder.appName("xr_retailer").getOrCreate()
spark.sql("use aistrong")


# ------------------------------------------  retail（零售户分析）表 统计类信息--------------------------------

hbase["table"]="TOBACCO.RETAIL"
hbase["familys"]="0"

# -----------------------获取co_co_01
co_co_01 = spark.sql("select  cust_id,qty_sum,amt_sum,born_date from DB2_DB2INST1_CO_CO_01") \
    .withColumn("born_date", to_date("born_date", "yyyyMMdd")) \
    .withColumn("today", current_date()) \
    .withColumn("week_diff", week_diff("today","born_date"))\
    .withColumn("month_diff",month_diff_udf(year(col("born_date")),month(col("born_date")),year(col("today")),month(col("today"))))\
    .where(col("month_diff") <= 2).coalesce(10)
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
        day = days[i]
        sum_colName = cols0["sum"][i]
        amount_colName = cols0["amount"][i]
        price_colName = cols0["price"][i]
        orders_colName = cols0["orders"][i]

        print(f"{day} 订货总量")
        if i < 3:
            day_filter = co_co_01.where((col("week_diff") > 0) & (col("week_diff") <= day))
        else:
            day_filter = co_co_01.where(col("month_diff") == day)

        # 某零售户上一周/上两周/上四周/上个月订货总量
        order_total = day_filter \
            .groupBy("cust_id") \
            .agg(sum("qty_sum").alias(sum_colName))
        order_total.foreachPartition(lambda x: write_hbase1(x, [sum_colName]))

        print(f"{day} 订货金额")
        # 某零售户上一周/上两周/上四周/上个月的订货金额
        amount_total = day_filter \
            .groupBy("cust_id") \
            .agg(sum("amt_sum").alias(amount_colName))
        amount_total.foreachPartition(lambda x: write_hbase1(x, [amount_colName]))

        print(f"{day} 订货条均价")
        # 某零售户上一周/上两周/上四周/上个月的订货条均价
        avg_price = order_total.join(amount_total, "cust_id") \
            .withColumn(price_colName, divider_udf(col(amount_colName), col(sum_colName)))
        avg_price.foreachPartition(lambda x: write_hbase1(x, [price_colName]))

        print(f"{day} 订单数")
        # 某零售户上一周/上两周/上四周/上个月的订单数
        day_filter.groupBy("cust_id") \
            .count() \
            .withColumnRenamed("count", orders_colName) \
            .foreachPartition(lambda x: write_hbase1(x, [orders_colName]))



        # -------------------------------------环比-----------------------------------
        # (1,2] (1,3] (1,5] (1,2]
        if i < 3:
            day_filter = co_co_01.where((col("week_diff") > 1) & (col("week_diff") <= (1 + day)))
        else:
            day_filter = co_co_01.where((col("month_diff") > 1) & (col("month_diff") <= (1 + day)))

        # 某零售户上次同期   订货总量
        ring_order_total = day_filter \
            .groupBy("cust_id") \
            .agg(sum("qty_sum").alias("ring_qty_ord"))
        # 某零售户上次同期   订货总金额
        ring_amount_total = day_filter \
            .groupBy("cust_id") \
            .agg(sum("amt_sum").alias("ring_amt_sum"))
        # 某零售户上次同期   订货均价
        ring_avg_price = ring_order_total.join(ring_amount_total, "cust_id") \
            .withColumn("ring_avg_price", divider_udf(col("ring_amt_sum"), col("ring_qty_ord")))


        ring_sum_colName = cols0["ring_sum"][i]
        ring_amount_colName = cols0["ring_amount"][i]
        ring_price_colName = cols0["ring_price"][i]

        print(f"{day} 订货总量环比变化情况")
        # 某零售户上一周/上两周/上四周/上个月订货总量环比变化情况
        order_ring_ratio = ring_order_total.join(order_total, "cust_id") \
            .withColumn(ring_sum_colName, period_udf(col(sum_colName), col("ring_qty_ord")))
        order_ring_ratio.foreachPartition(lambda x: write_hbase1(x, [ring_sum_colName]))

        print(f"{day} 订货总金额环比变化情况")
        # 某零售户上一周/上两周/上四周/上个月订货总金额环比变化情况
        amount_ring_ratio = ring_amount_total.join(amount_total, "cust_id") \
            .withColumn(ring_amount_colName, period_udf(col(amount_colName), col("ring_amt_sum")))
        amount_ring_ratio.foreachPartition(lambda x: write_hbase1(x, [ring_amount_colName]))

        print(f"{day} 订货条均价环比变化情况")
        # 某零售户上一周/上两周/上四周/上个月订货条均价环比变化情况
        avg_price_ring_ratio = ring_avg_price.join(avg_price, "cust_id") \
            .withColumn(ring_price_colName, period_udf(col(price_colName), col("ring_avg_price")))
        avg_price_ring_ratio.foreachPartition(lambda x: write_hbase1(x, [ring_price_colName]))



# -------------------获取co_co_line
co_co_line = spark.sql("select cust_id,item_id,qty_ord,price,born_date from DB2_DB2INST1_CO_CO_LINE") \
    .withColumn("born_date", to_date("born_date", "yyyyMMdd")) \
    .withColumn("today", current_date()) \
    .withColumn("week_diff", week_diff("today","born_date"))\
    .withColumn("month_diff",month_diff_udf(year(col("born_date")),month(col("born_date")),year(col("today")),month(col("today"))))\
    .where(col("month_diff") <= 12)

# ------------------获取plm_item
plm_item = spark.sql("select item_id,yieldly_type,kind from DB2_DB2INST1_PLM_ITEM").coalesce(5).cache()

line_plm = co_co_line.join(plm_item, "item_id")

# 总量
#上1周
total_week_1 = co_co_line.where(col("week_diff") == 1) \
    .groupBy("cust_id") \
    .agg(sum("qty_ord").alias("qty_ord")).coalesce(10)

filter_this_month=co_co_line.where(col("month_diff") > 0)
#上个月
total_month_1 = filter_this_month.where(col("month_diff") <= 1) \
    .groupBy("cust_id") \
    .agg(sum("qty_ord").alias("qty_ord")).coalesce(10)
#上3个月
total_month_3 = filter_this_month.where(col("month_diff") <= 3) \
    .groupBy("cust_id") \
    .agg(sum("qty_ord").alias("qty_ord")).coalesce(10)
#上6个月
total_month_6 = filter_this_month.where(col("month_diff") <= 6) \
    .groupBy("cust_id") \
    .agg(sum("qty_ord").alias("qty_ord")).coalesce(10)
#上12个月
total_month_12 = filter_this_month.where(col("month_diff") <= 12) \
    .groupBy("cust_id") \
    .agg(sum("qty_ord").alias("qty_ord")).coalesce(10)





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

win = Window.partitionBy("cust_id").orderBy(desc("yieldly_type_qty_ord"))

for key in days.keys():
    #对应日期的烟总量
    total_df=days_total[key]
    #日期过滤条件
    day = days[key]
    if key == "1_week":
        day_filter = line_plm.where(col("week_diff") == day)
    else:
        day_filter = line_plm.where((col("month_diff") >0) & (col("month_diff") <= day))

    # 某零售户上1周，上个月，上3个月，上6个月，上12个月所订省内烟、省外烟、进口烟的占比（省内外烟）
    for i in range(len(yieldly_types)):
        yieldly_type = yieldly_types[i]
        yieldly_type_filter = day_filter.where(col("yieldly_type") == yieldly_type)
        colName = cols1[key][i]
        try:
            print(f"{str(dt.now())}  yieldly_type:{yieldly_type} {key}:{day}")
            yieldly_type_filter.groupBy("cust_id") \
                .agg(sum("qty_ord").alias("yieldly_type_qty_ord")) \
                .join(total_df, "cust_id") \
                .withColumn(colName, divider_udf(col("yieldly_type_qty_ord"), col("qty_ord"))) \
                .foreachPartition(lambda x: write_hbase1(x, [colName]))

            # top 5
            # 零售户上1周，上个月，上3个月，上6个月，上12个月订购数前5省内烟
            # 零售户上1周，上个月，上3个月，上6个月，上12个月订购数前5省外烟
            if yieldly_type in ["0", "1"]:
                    print(f"{str(dt.now())}  top5   yieldly_type:{yieldly_type} {key}:{day}")
                    colName = cols2[key][i]
                    yieldly_type_filter.coalesce(5).groupBy("cust_id", "item_id") \
                        .agg(sum("qty_ord").alias("yieldly_type_qty_ord")) \
                        .withColumn("rank", row_number().over(win)) \
                        .where(col("rank") <= 5) \
                        .groupBy("cust_id") \
                        .agg(collect_list("item_id").alias(colName)) \
                        .foreachPartition(lambda x: write_hbase1(x, [colName]))
        except Exception as e:
            print(f"error   yieldly_type:{yieldly_type} {key}:{day}")
            print(e.args)
    # 某零售户上1周，上个月，上3个月，上6个月，上12个月所订卷烟价类占比
    for i in range(len(kinds)):
        kind=kinds[i]
        kind_filter = day_filter.where(col("kind") == kind)
        colName = cols3[key][i]
        print(f"{str(dt.now())}  kind:{kind} {key}:{day}")
        try:
            kind_filter.groupBy("cust_id") \
                .agg(sum("qty_ord").alias("kind_qty_ord")) \
                .join(total_df, "cust_id") \
                .withColumn(colName, divider_udf(col("kind_qty_ord"), col("qty_ord")))\
                .foreachPartition(lambda x: write_hbase1(x, [colName]))

        except Exception as e:
            print(f"error   kind:{kind},day:{day}")
            print(e.args)



# 某零售户上1周，上个月，上3个月，上6个月，上12个月所订卷烟价格分段占比
# 50以下；50（含）-100，100（含）-300，300（含）-500，500（含）以上
cols4={
    "1_week":["price_sub_last_week_under50","price_sub_last_week_50_to_100","price_sub_last_week_100_to_300","price_sub_last_week_300_to_500","price_sub_last_week_up500"],
    "1_month":["price_sub_last_month_under50","price_sub_last_month_50_to_100","price_sub_last_month_100_to_300","price_sub_last_month_300_to_500","price_sub_last_month_up500"],
    "3_month":["price_sub_last_three_month_under50","price_sub_last_three_month_50_to_100","price_sub_last_three_month_100_to_300","price_sub_last_three_month_300_to_500","price_sub_last_three_month_up500"],
    "6_month":["price_sub_last_half_year_under50","price_sub_last_half_year_50_to_100","price_sub_last_half_year_100_to_300","price_sub_last_half_year_300_to_500","price_sub_last_half_year_up500"],
    "12_month":["price_sub_last_year_under50","price_sub_last_year_50_to_100","price_sub_last_year_100_to_300","price_sub_last_year_300_to_500","price_sub_last_year_up500"]
}
prices =[0,50,100,300,500]

for key in days.keys():
    # 对应日期的烟总量
    total_df = days_total[key]
    # 日期过滤条件
    day = days[key]
    if key == "1_week":
        day_filter = co_co_line.where(col("week_diff") == day)
    else:
        day_filter = co_co_line.where((col("month_diff") > 0) & (col("month_diff") <= day))

    for i in range(len(prices)):
        price=prices[i]
        colName = cols4[key][i]
        if price ==500:
            price_filter = day_filter.where(col("price") >= price)
            print(f"{str(dt.now())}  price:({price},∞],{key}:{day}")
        else:
            price_filter = day_filter.where((col("price") >= price) & (col("price") < prices[i + 1]))
            print(f"{str(dt.now())}  price:({price},{prices[i+1]}],{key}:{day}")
        try:


            price_filter.groupBy("cust_id") \
                    .agg(sum("qty_ord").alias("price_qty_ord")) \
                    .join(total_df, "cust_id", "left") \
                    .withColumn(colName, divider_udf(col("price_qty_ord"), col("qty_ord"))) \
                    .foreachPartition(lambda x: write_hbase1(x, [colName]))
        except Exception as e:
            print(f"error   price:({price},{prices[i+1]}],{key}:{day}")
            print(e.args)

#-------------co_co_01  去年
last_year = spark.sql("select  cust_id,qty_sum,amt_sum,born_date from DB2_DB2INST1_CO_CO_01") \
    .withColumn("born_date", to_date("born_date", "yyyyMMdd")) \
    .withColumn("last_year_today", date_sub(current_date(), 365)) \
    .withColumn("month_diff",month_diff_udf(year(col("born_date")),month(col("born_date")),year(col("last_year_today")),month(col("last_year_today"))))\
    .where(col("month_diff") == 1)
try:
    # 某零售户上个月订货总量同比变化情况
    # 上个月
    last_month_order_sum = co_co_01.where(col("month_diff") == 1) \
        .groupBy("cust_id") \
        .agg(sum("qty_sum").alias("qty_sum"))
    # 去年同期
    last_year_order_sum = last_year.groupBy("cust_id") \
        .agg(sum("qty_sum").alias("last_year_qty_sum"))

    colName="sum_lyear"
    print("某零售户上个月订货总量同比变化情况  ",colName)
    last_year_order_sum.join(last_month_order_sum, "cust_id") \
        .withColumn(colName, period_udf(col("qty_sum"), col("last_year_qty_sum")))\
        .foreachPartition(lambda x: write_hbase1(x, [colName]))

    # 某零售户上个月订货总金额同比变化情况
    # 上个月
    last_month_moy_sum = co_co_01.where(col("month_diff") == 1) \
        .groupBy("cust_id") \
        .agg(sum("amt_sum").alias("amt_sum"))
    # 去年同期
    last_year_moy_sum = last_year.groupBy("cust_id") \
        .agg(sum("amt_sum").alias("last_year_amt_sum"))
    colName="amount_lyear"
    print("某零售户上个月订货总金额同比变化情况  ", colName)
    last_year_moy_sum.join(last_month_moy_sum, "cust_id") \
        .withColumn(colName, period_udf(col("amt_sum"), col("last_year_amt_sum")))\
        .foreachPartition(lambda x: write_hbase1(x, [colName]))

    # 某零售户上个月订货条均价同比变化情况
    # 上个月
    last_month_ratio = last_month_order_sum.join(last_month_moy_sum, "cust_id") \
        .withColumn("avg_price", divider_udf(col("amt_sum"), col("qty_sum")))
    # 去年同期
    last_year_ratio = last_year_order_sum.join(last_year_moy_sum, "cust_id") \
        .withColumn("last_year_avg_price", divider_udf(col("last_year_amt_sum"), col("last_year_qty_sum")))
    colName="price_lyear"
    print("某零售户上个月订货条均价同比变化情况  ", colName)
    last_year_ratio.join(last_month_ratio, "cust_id") \
        .withColumn(colName, period_udf(col("avg_price"), col("last_year_avg_price")))\
        .foreachPartition(lambda x: write_hbase1(x, [colName]))
except Exception as e:
    print(e.args,colName)

# --------------------------------------------------------retail（零售户信息）基本信息---------------------------------------------
hbase["table"]="TOBACCO.RETAIL"


# -----------------co_cust 零售客户信息表
# 需要修改的列名   与hbase的列名对应
co_cust_retail = {"is_tor_tax": "tor_tax", "is_sale_large": "sale_large",
                  "is_rail_cust": "rail_cust", "is_sefl_cust": "sefl_cust",
                  "is_func_cust": "func_cust", "cust_seg": "grade"
                  }
"""
cust_id, cust_seg, status, pay_type, license_code,
manager, identity_card_id, order_tel, inv_type, order_way,
periods, busi_addr, work_port, base_type, sale_scope,
scope, com_chara, is_tor_tax, is_sale_large, is_rail_cust,
area_type, is_sefl_cust, is_func_cust
"""

co_cust = spark.sql("select cust_id,cust_seg,status,pay_type,license_code,manager,identity_card_id,order_tel,"
                    "inv_type,order_way,periods,busi_addr,work_port,base_type,sale_scope,scope,com_chara,"
                    "is_tor_tax,is_sale_large,is_rail_cust,area_type,is_sefl_cust,is_func_cust,dt from DB2_DB2INST1_CO_CUST "
                    "where dt=(select max(dt) from DB2_DB2INST1_CO_CUST)")


for key in co_cust_retail.keys():
    co_cust = co_cust.withColumnRenamed(key, co_cust_retail[key])

cols = co_cust.columns

co_cust.foreachPartition(lambda x: write_hbase1(x, cols))

# city county abcode
co_cust=spark.sql("select cust_id,com_id,sale_center_id,dt from DB2_DB2INST1_CO_CUST where dt=(select max(dt) from DB2_DB2INST1_CO_CUST)").coalesce(5)
area_code=spark.read.csv(path="/user/entrobus/zhangzy/区县名称与sale_center_id匹配关系0410+区域编码.csv",header=True)
co_cust.join(area_code,["com_id","sale_center_id"])\
       .withColumnRenamed("城市","city")\
       .withColumnRenamed("区","county")\
       .withColumnRenamed("sale_center_id","abcode")\
       .foreachPartition(lambda x: write_hbase1(x, ["abcode","city","county"]))


# ----------------crm_cust 零售客户信息表
# 需要修改的列名
crm_cust_retail = {"crm_longitude": "longitude", "crm_latitude": "latitude",
                   "is_multiple_shop": "multiple_shop", "is_night_shop": "night_shop"}
crm_cust = spark.sql(
    "select cust_id,crm_longitude,crm_latitude,is_multiple_shop,org_model,is_night_shop,busi_time_type,consumer_group,consumer_attr,busi_type,compliance_grade from DB2_DB2INST1_CRM_CUST")

for key in crm_cust_retail.keys():
    crm_cust = crm_cust.withColumnRenamed(key, crm_cust_retail[key])
cols = crm_cust.columns
crm_cust.foreachPartition(lambda x: write_hbase1(x, cols))

# ----------------crm_cust_log 变更记录表
crm_cust_log = spark.sql("select cust_id,change_type,change_frm,change_to,audit_date from DB2_DB2INST1_CRM_CUST_LOG") \
    .withColumn("audit_date", to_date("audit_date", "yyyyMMdd")) \
    .where(datediff(current_date(), col("audit_date")) <= 30)
# 档位变更
cust_seg = crm_cust_log.where(col("change_type") == "CO_CUST.CUST_SEG")
# 前30天档位变更次数
cust_seg.groupBy("cust_id") \
    .agg(count("change_type").alias("grade_change_count")) \
    .foreachPartition(lambda x: write_hbase1(x, ["grade_change_count"]))


# 前30天档位变更差
def diff(max1, min1, max2, min2):
    l = sorted([int(max1), int(min1), int(max2), int(min2)])
    diff = l[3] - l[0]
    return diff
diff_udf = udf(diff)
cust_seg.groupBy("cust_id") \
    .agg(diff_udf(max("change_frm"), min("change_frm"), max("change_to"), min("change_to")).alias("grade_abs")) \
    .foreachPartition(lambda x: write_hbase1(x, ["grade_abs"]))

# 30天前档位
grade_before = cust_seg.groupBy("cust_id") \
    .agg(min("audit_date").alias("audit_date")) \
    .join(cust_seg, ["cust_id", "audit_date"]) \
    .withColumnRenamed("change_frm", "grade_before") \
    .foreachPartition(lambda x: write_hbase1(x, ["grade_before"]))

# 状态变更
status = crm_cust_log.where(col("change_type") == "CO_CUST.STATUS")

# 前30天状态变更次数
status.groupBy("cust_id") \
    .agg(count("change_type").alias("status_change_count")) \
    .foreachPartition(lambda x: write_hbase1(x, ["status_change_count"]))
# 前30天状态
status_before = status.groupBy("cust_id") \
    .agg(min("audit_date").alias("audit_date")) \
    .join(status, ["cust_id", "audit_date"]) \
    .select("cust_id", "change_frm") \
    .withColumnRenamed("change_frm", "status_before") \
    .foreachPartition(lambda x: write_hbase1(x, ["status_before"]))


# 是否存在实际经营人与持证人不符
def is_match(x, y):
    if x == y:
        result = "1"
    else:
        result = "0"
    return result


isMatch = card_pass = udf(is_match)
co_cust = spark.sql("select cust_id,identity_card_id,dt from DB2_DB2INST1_CO_CUST where dt=(select max(dt) from DB2_DB2INST1_CO_CUST)").coalesce(5)
co_debit_acc = spark.sql("select cust_id,pass_id from DB2_DB2INST1_CO_DEBIT_ACC").coalesce(5)
co_cust.join(co_debit_acc, "cust_id") \
    .withColumn("license_not_match", isMatch(col("identity_card_id"), col("pass_id"))) \
    .foreachPartition(lambda x: write_hbase1(x, ["license_not_match"]))
# 是否隐形连锁户
