#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/4/8 15:29
from pyspark.sql.functions import col,to_date,current_date,datediff,udf,desc,row_number,collect_list,date_sub,count,year,month,date_trunc,sum
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pysparkDemo.rules.write_hbase import hbase,write_hbase1
from datetime import datetime as dt
from pysparkDemo.rules.utils import month_diff_udf,divider_udf,period_udf,week_diff
spark = SparkSession.builder.appName("xr_retailer").getOrCreate()
spark.sql("use aistrong")

#--------------------------------------------area表（区域画像）-----------------------------------------------------
hbase["table"]="TOBACCO.AREA"
hbase["row"]="sale_center_id"
# -----------------------获取co_co_01
co_co_01 = spark.sql("select  qty_sum,amt_sum,born_date,sale_center_id from DB2_DB2INST1_CO_CO_01") \
    .withColumn("born_date", to_date("born_date", "yyyyMMdd")) \
    .withColumn("today", current_date()) \
    .withColumn("week_diff", week_diff("today","born_date"))\
    .withColumn("month_diff",month_diff_udf(year(col("born_date")),month(col("born_date")),year(col("today")),month(col("today"))))\
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
    print(f"order sum : {day}")
    # 本县或区所有零售户上一周 / 上两周 / 上四周 / 上个月 / 本月截止订货总量
    order_sum = day_filter.select("sale_center_id", "qty_sum") \
        .groupBy("sale_center_id") \
        .agg(sum("qty_sum").alias(sum_colName))
    order_sum.foreachPartition(lambda x: write_hbase1(x, [sum_colName]))

    print(f"amount sum : {day}")
    # 本县或区所有零售户上一周 / 上两周 / 上四周 / 上个月 / 本月截止订货金额总和
    amount_total = day_filter.select("sale_center_id", "amt_sum") \
        .groupBy("sale_center_id") \
        .agg(sum("amt_sum").alias(amount_colName))
    amount_total.foreachPartition(lambda x: write_hbase1(x, [amount_colName]))

    print(f"avg price : {day}")
    # 本县或区所有零售户上一周 / 上两周 / 上四周 / 上个月订货条均价
    avg_price = order_sum.join(amount_total, "sale_center_id") \
        .withColumn(price_colName, divider_udf(col(amount_colName), col(sum_colName)))
    avg_price.foreachPartition(lambda x: write_hbase1(x, [price_colName]))

    print(f"order count : {day}")
    # 本县或区所有零售户上一周 / 上两周 / 上四周 / 上个月订单数总和
    order_count=day_filter.groupBy("sale_center_id") \
        .count() \
        .withColumnRenamed("count", orders_colName)
    order_count.foreachPartition(lambda x: write_hbase1(x, [orders_colName]))


    #--------------------------------------环比------------------------------------------
    # 上一周 上两周 上四周 上个月 环比情况
    # (1,2] (1,3] (1,5] (1,2]
    if i < 4:
        if i < 3:
            day_filter = co_co_01.where((col("week_diff") > 1) & (col("week_diff") <= (1 + day)))
        else:
            day_filter = co_co_01.where((col("month_diff") > 1) & (col("month_diff") <= (1 + day)))

        # 本县或区所有零售户订货总量   上次同期
        ring_order_sum = day_filter.select("sale_center_id", "qty_sum") \
            .groupBy("sale_center_id") \
            .agg(sum("qty_sum").alias("ring_qty_sum"))

        # 本县或区所有零售户订货金额总和  上次同期
        ring_amount_total = day_filter.select("sale_center_id", "amt_sum") \
            .groupBy("sale_center_id") \
            .agg(sum("amt_sum").alias("ring_amt_sum"))

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


        print(f"订货总量环比变化情况 {day}")
        # 本县或区 上一周/上两周/上四周/上个月订单总量环比变化情况
        order_sum_ring_ratio = ring_order_sum.join(order_sum, "sale_center_id") \
            .withColumn(ring_sum_colName, period_udf(col(sum_colName), col("ring_qty_sum")))
        order_sum_ring_ratio.foreachPartition(lambda x: write_hbase1(x, [ring_sum_colName]))

        print(f"订货总金额环比变化情况 {day}")
        # 本县或区 上一周/上两周/上四周/上个月订货总金额环比变化情况
        amount_ring_ratio = ring_amount_total.join(amount_total, "sale_center_id") \
            .withColumn(ring_amount_colName, period_udf(col(amount_colName), col("ring_amt_sum")))
        amount_ring_ratio.foreachPartition(lambda x: write_hbase1(x, [ring_amount_colName]))

        print(f"订货条均价环比变化情况 {day}")
        # 本县或区 上一周/上两周/上四周/上个月订货条均价环比变化情况
        avg_price_ring_ratio = ring_avg_price.join(avg_price, "sale_center_id") \
            .withColumn(ring_price_colName, period_udf(col(price_colName), col("ring_avg_price")))
        avg_price_ring_ratio.foreachPartition(lambda x: write_hbase1(x, [ring_price_colName]))

        print(f"订单数环比变化情况 {day}")
        order_count_ring_ratio = ring_order_count.join(order_count, "sale_center_id") \
            .withColumn(ring_order_colName, period_udf(col(orders_colName), col("ring_count")))
        order_count_ring_ratio.foreachPartition(lambda x: write_hbase1(x, [ring_order_colName]))


#-------------co_co_01  去年  同比情况
last_year = spark.sql("select  qty_sum,amt_sum,sale_center_id,born_date from DB2_DB2INST1_CO_CO_01") \
    .withColumn("born_date", to_date("born_date", "yyyyMMdd")) \
    .withColumn("last_year_today", date_sub(current_date(), 365)) \
    .withColumn("month_diff",month_diff_udf(year(col("born_date")),month(col("born_date")),year(col("last_year_today")),month(col("last_year_today"))))\
    .where(col("month_diff") == 1)


try:
    print("本县或区所有零售户上个月订货总量同比变化情况")
    # 本县或区所有零售户上个月订货总量同比变化情况
    # 上个月
    last_month_qty_sum = co_co_01.where(col("month_diff") == 1) \
        .groupBy("sale_center_id") \
        .agg(sum("qty_sum").alias("qty_sum"))
    # 去年同期
    last_year_qty_sum = last_year.groupBy("sale_center_id") \
        .agg(sum("qty_sum").alias("last_year_qty_sum"))
    #订货总量同比
    colName="sum_lyear"
    last_year_qty_sum.join(last_month_qty_sum, "sale_center_id") \
        .withColumn(colName, period_udf(col("qty_sum"), col("last_year_qty_sum")))\
        .foreachPartition(lambda x:write_hbase1(x,[colName]))

    print("本县或区所有零售户上个月订货总金额同比变化情况")
    # 本县或区所有零售户上个月订货总金额同比变化情况
    # 上个月
    last_month_amt_sum = co_co_01.where(col("month_diff") == 1) \
        .groupBy("sale_center_id") \
        .agg(sum("amt_sum").alias("amt_sum"))
    # 去年同期
    last_year_amt_sum = last_year.groupBy("sale_center_id") \
        .agg(sum("amt_sum").alias("last_year_amt_sum"))
    #订货总金额同比
    colName="amount_lyear"
    last_year_amt_sum.join(last_month_amt_sum, "sale_center_id") \
        .withColumn(colName, period_udf(col("amt_sum"), col("last_year_amt_sum")))\
        .foreachPartition(lambda x:write_hbase1(x,[colName]))

    print("本县或区所有零售户上个月订货条均价同比变化情况")
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
        .foreachPartition(lambda x:write_hbase1(x,[colName]))

    print("本县或区所有零售户上个月订单数同比变化情况")
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
        .foreachPartition(lambda x:write_hbase1(x,[colName]))
except Exception as e:
    print(e.args)
    print("colName:",colName)





# -------------------获取co_co_line
co_co_line = spark.sql("select item_id,qty_ord,price,sale_center_id,born_date from DB2_DB2INST1_CO_CO_LINE") \
    .withColumn("born_date", to_date("born_date", "yyyyMMdd")) \
    .withColumn("today", current_date()) \
    .withColumn("week_diff", week_diff("today","born_date"))\
    .withColumn("month_diff",month_diff_udf(year(col("born_date")),month(col("born_date")),year(col("today")),month(col("today"))))\
    .where(col("month_diff") <= 12)

# ------------------获取plm_item
plm_item = spark.sql("select item_id,yieldly_type,kind from DB2_DB2INST1_PLM_ITEM "
                     "where dt=(select max(dt) from DB2_DB2INST1_PLM_ITEM)").coalesce(1)

line_plm = co_co_line.join(plm_item, "item_id").coalesce(10).cache()

# 总量
#上1周
total_week_1 = co_co_line.where(col("week_diff") == 1) \
    .groupBy("sale_center_id") \
    .agg(sum("qty_ord").alias("qty_ord")).coalesce(10)

filter_this_month=co_co_line.where(col("month_diff") > 0)
#上个月
total_month_1 = filter_this_month.where(col("month_diff") <= 1) \
    .groupBy("sale_center_id") \
    .agg(sum("qty_ord").alias("qty_ord")).coalesce(10)
#上3个月
total_month_3 = filter_this_month.where(col("month_diff") <= 3) \
    .groupBy("sale_center_id") \
    .agg(sum("qty_ord").alias("qty_ord")).coalesce(10)
#上6个月
total_month_6 = filter_this_month.where(col("month_diff") <= 6) \
    .groupBy("sale_center_id") \
    .agg(sum("qty_ord").alias("qty_ord")).coalesce(10)
#上12个月
total_month_12 = filter_this_month.where(col("month_diff") <= 12) \
    .groupBy("sale_center_id") \
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

win = Window.partitionBy("sale_center_id").orderBy(desc("yieldly_type_qty_ord"))

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
            print(f"yieldly_type:{yieldly_type} {key}:{day}")
            yieldly_type_filter.groupBy("sale_center_id") \
                .agg(sum("qty_ord").alias("yieldly_type_qty_ord")) \
                .join(total_df, "sale_center_id") \
                .withColumn(colName, divider_udf(col("yieldly_type_qty_ord"), col("qty_ord"))) \
                .foreachPartition(lambda x: write_hbase1(x, [colName]))

            # top 5
            # 本县或区所有零售户上1周，上个月，上3个月，上6个月，上12个月订购数前5省内烟
            # 零售户上1周，上个月，上3个月，上6个月，上12个月订购数前5省外烟
            if yieldly_type in ["0", "1"]:
                    print(f"top5   yieldly_type:{yieldly_type} {key}:{day}")
                    colName = cols2[key][i]
                    yieldly_type_filter.coalesce(5).groupBy("sale_center_id", "item_id") \
                        .agg(sum("qty_ord").alias("yieldly_type_qty_ord")) \
                        .withColumn("rank", row_number().over(win)) \
                        .where(col("rank") <= 5) \
                        .groupBy("sale_center_id") \
                        .agg(collect_list("item_id").alias(colName)) \
                        .foreachPartition(lambda x: write_hbase1(x, [colName]))
        except Exception as e:
            print(f"error   yieldly_type:{yieldly_type} {key}:{day}")
            print(e.args)
    # 本县或区所有零售户上1周，上个月，上3个月，上6个月，上12个月所订卷烟价类占比
    for i in range(len(kinds)):
        kind=kinds[i]
        kind_filter = day_filter.where(col("kind") == kind)
        colName = cols3[key][i]
        print(f"kind:{kind} {key}:{day}")
        try:
            kind_filter.groupBy("sale_center_id") \
                .agg(sum("qty_ord").alias("kind_qty_ord")) \
                .join(total_df, "sale_center_id") \
                .withColumn(colName, divider_udf(col("kind_qty_ord"), col("qty_ord")))
        except Exception as e:
            print(f"error   kind:{kind},day:{day}")
            print(e.args)


# 本县或区所有零售户上1周，上个月，上3个月，上6个月，上12个月所订卷烟价格分段占比
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

            price_filter.groupBy("sale_center_id") \
                    .agg(sum("qty_ord").alias("price_qty_ord")) \
                    .join(total_df, "sale_center_id", "left") \
                    .withColumn(colName, divider_udf(col("price_qty_ord"), col("qty_ord")))\
                    .foreachPartition(lambda x:write_hbase1(x,[colName]))
        except Exception as e:
            print(f"error   price:{price},{key}:{day}")
            print(e.args)
