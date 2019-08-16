#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/4/17 15:27
import traceback as tb
from pyspark.sql import functions as f
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql import Window
from datetime import datetime as dt
from rules.write_hbase import write_hbase1
from rules.utils import *
from rules.config import retail_warning_table
"""
邵阳预警
"""
spark = SparkSession.builder.enableHiveSupport().appName("retail warning").getOrCreate()
sc=spark.sparkContext
sc.setLogLevel("WARN")

spark.sql("use aistrong")

hbase={"table":retail_warning_table,"families":["0"],"row":"cust_id"}
# hbase={"table":"test1","families":["0"],"row":"cust_id"}



#-----零售店名
def get_cust_name():
    get_valid_co_cust(spark).select("cust_id","cust_name")\
                  .foreachPartition(lambda x:write_hbase1(x,["cust_name"],hbase))
# get_cust_name()




#-----市 区 sale_center_id
def get_city_county():
    print(f"{str(dt.now())} 市 区 abcode")
    try:
        co_cust=get_valid_co_cust(spark).select("cust_id","com_id","sale_center_id")

        area_code=get_area(spark)

        co_cust.join(area_code,["com_id","sale_center_id"])\
               .withColumnRenamed("城市","city")\
               .withColumnRenamed("区","county")\
               .foreachPartition(lambda x: write_hbase1(x, ["sale_center_id","city","county"],hbase))
    except Exception as e:
        tb.print_exc()

# get_city_county()



#-----经纬度
def get_lng_lat():
    print(f"{str(dt.now())}  经纬度")
    try:
        co_cust=get_valid_co_cust(spark).select("cust_id")
        crm_cust =get_crm_cust(spark).select("cust_id","longitude","latitude")

        co_cust.join(crm_cust,"cust_id")\
               .foreachPartition(lambda x:write_hbase1(x,["longitude","latitude"],hbase))
    except Exception:
        tb.print_exc()

    #-----网上爬取的经纬度
    print(f"{str(dt.now())}   经纬度")
    try:
        cust_lng_lat=get_cust_lng_lat(spark).select("cust_id","lng","lat")
        co_cust.join(cust_lng_lat,"cust_id")\
            .withColumnRenamed("lng","longitude")\
            .withColumnRenamed("lat","latitude")\
            .foreachPartition(lambda x:write_hbase1(x,["longitude","latitude"],hbase))
    except Exception as e:
       tb.print_exc()

# get_lng_lat()





# def is_match(x, y):
#     if x != y:
#         result = "1"
#     return result
# isMatch = card_pass = f.udf(is_match)
def one_card_multi_cust():
    try:
        # 过滤掉 无效 暂停  特殊用户  中石化 中石油 中海油
        valid_co_cust = get_valid_co_cust(spark) \
            .where((col("cust_seg") != "ZZ") & (col("cust_name").rlike("^(?!.*中石油|.*中石化|.*中海油|.*中国石化|.*中国石油)"))) \
            .select("cust_id","identity_card_id","order_tel")
        # 过滤掉连锁店
        crm_cust = get_crm_cust(spark).where(col("is_multiple_shop") == "0").select("cust_id")

        co_cust=valid_co_cust.join(crm_cust,"cust_id")

        # 过滤掉了无效银行卡
        co_debit_acc = get_co_debit_acc(spark).select("cust_id", "acc")

        cust_debit_acc=co_cust.join(co_debit_acc,"cust_id")


        #移除
        # colName="license_not_match"
        # # -----实际经营人与持证人不符
        # cust_debit_acc.withColumn(colName, isMatch(col("identity_card_id"), col("pass_id"))) \
        #     .where(col(colName)=="1")\
        #     .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))


        # -----每个身份证对应 多零售户id
        try:
            print(f"{str(dt.now())}  每个身份证对应 多零售户id")
            card_filter=co_cust.where(col("identity_card_id").rlike("(\w{15,})"))
            cust_num=card_filter.groupBy("identity_card_id")\
                            .agg(f.count("cust_id").alias("cust_num"))\
                            .where(col("cust_num")>1)
            colName="one_id_more_retail"
            card_filter.join(cust_num,"identity_card_id")\
                    .withColumn(colName,f.lit("1"))\
                    .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))
        except Exception as e:
            tb.print_exc()


        #-----每个电话对应 多零售户id
        try:
            print(f"{str(dt.now())}  每个电话对应 多零售户id")
            tel_filter=co_cust.where(col("order_tel").rlike("[^(NULL)]"))
            cust_num=tel_filter.groupBy("order_tel")\
                            .agg(f.count("cust_id").alias("cust_num"))\
                            .where(col("cust_num")>1)
            colName="one_tel_more_retail"
            tel_filter.join(cust_num,"order_tel")\
                    .withColumn(colName,f.lit("1"))\
                    .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))
        except Exception as e:
            tb.print_exc()


        #-----一个银行账号对应多个零售户
        try:
            print(f"{str(dt.now())}  一个银行账号对应多个零售户 多零售户id")
            cust_num=cust_debit_acc.groupBy("acc")\
                        .agg(f.count("cust_id").alias("cust_num"))\
                        .where(col("cust_num")>1)
            colName="one_bank_more_retail"
            co_debit_acc.join(cust_num,"acc")\
                        .withColumn(colName,f.lit("1"))\
                        .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))
        except Exception as e:
            tb.print_exc()

        # -----近一个月内一订货IP地址多零售店ID
        try:
            """
            co_log: log_seq ip_addr
            co_log_line: log_seq co_num
            co_co_line: co_num cust_id
            """

            print(f"{str(dt.now())}  近一个月内一订货IP地址多零售店ID")
            co_log = spark.sql(
                "select log_seq,ip_addr,log_date from DB2_DB2INST1_CO_LOG where log_status='02'") \
                .where(col("ip_addr").rlike("[^(NULL)\s]")) \
                .withColumn("log_date", f.to_date(col("log_date"), "yyyyMMdd")) \
                .withColumn("today", f.current_date()) \
                .withColumn("day_diff", f.datediff(col("today"), col("log_date"))) \
                .where(col("day_diff") <= 30)

            co_log_line = spark.sql(
                "select log_seq,co_num,update_time from DB2_DB2INST1_CO_LOG_LINE where pmt_status='1'") \
                .withColumn("update_time", f.to_date(col("update_time"), "yyyy-MM-dd HH:mm:ss")) \
                .withColumn("today", f.current_date()) \
                .withColumn("day_diff", f.datediff(col("today"), col("update_time"))) \
                .where(col("day_diff") <= 30)

            co_co_line =get_co_co_line(spark,scope=[0,30]).select("cust_id","co_num") \
                .dropDuplicates(["co_num"]) \

            ip_addr = "order_ip_addr"
            cust_ip = co_log.join(co_log_line, "log_seq") \
                .join(co_co_line, "co_num") \
                .join(co_cust,"cust_id")\
                .select("cust_id", "ip_addr") \
                .dropDuplicates(["cust_id", "ip_addr"])
            colName = "one_ip_more_retail"
            cust_ip.groupBy("ip_addr") \
                .agg(f.count("cust_id").alias("cust_num")) \
                .where(col("cust_num") > 1) \
                .join(cust_ip, "ip_addr") \
                .withColumnRenamed("ip_addr", ip_addr) \
                .withColumn(colName, f.lit(1))\
                .foreachPartition(lambda x: write_hbase1(x, [colName, ip_addr], hbase))
        except Exception as e:
            tb.print_exc()
    except Exception as e:
        tb.print_exc()


# one_card_multi_cust()







#-----零售户90天内档位变更数值在箱式图的上限或下限之外
def get_grade_except():
    try:
        co_cust=get_valid_co_cust(spark).select("cust_id","com_id")
        #档位变更表
        crm_cust_log=get_crm_cust_log(spark).where((col("change_type")=="CO_CUST.CUST_SEG") & (col("day_diff")<=90))\
                        .select("cust_id","change_frm","change_to","audit_date")


        #档位变更差
        grade_diff_df=crm_cust_log.groupBy("cust_id")\
                   .agg(grade_diff_udf(f.max("change_frm"), f.min("change_frm"), f.max("change_to"), f.min("change_to")).alias("grade_diff"))\
                   .join(co_cust,"cust_id")
        grade_diff_df.registerTempTable("grade_diff_df")

        colName="grade_abno"
        print(f"{str(dt.now())}   档位变更差异常的")
        result=spark.sql("select com_id,percentile_approx(grade_diff,0.25) as percent_25,percentile_approx(grade_diff,0.75) as percent_75 from grade_diff_df group by com_id")\
                .join(grade_diff_df,"com_id")\
                .withColumn(colName,box_plots_filter_udf(col("grade_diff"),col("percent_25"),col("percent_75")))\
                .where(col(colName)>=0)
        result.foreachPartition(lambda x:write_hbase1(x,[colName],hbase))


        print(f"{str(dt.now())}  档位变更差")
        colName="grade_abs_three_month"
        grade_diff_df.join(result,"cust_id")\
                     .withColumnRenamed("grade_diff",colName)\
                     .foreachPartition(lambda x:write_hbase1(x,[colName],hbase))


        print(f"{str(dt.now())}   档位变更次数")
        colName="grade_change_times"
        crm_cust_log.join(result,"cust_id")\
                    .groupBy("cust_id").agg(f.count("cust_id").alias(colName))\
                    .foreachPartition(lambda x:write_hbase1(x,[colName],hbase))

        print(f"{str(dt.now())}   90天前档位")
        colName="grade_last_three_month"
        crm_cust_log.groupBy("cust_id") \
            .agg(f.min("audit_date").alias("audit_date")) \
            .join(crm_cust_log.join(result,"cust_id"), ["cust_id", "audit_date"]) \
            .withColumnRenamed("change_frm", colName) \
            .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))
    except Exception as e:
        tb.print_exc()


# get_grade_except()





"""
计算方法: 例如：零售店单月订购总量异常
  1.上个月零售店的订购量 value  得到df1    
  2.全市同档位零售店的订购量的均值(mean)/标准差(std)  :基于第一步再按com_id cust_seg分组，求均值、标准差   得到df2
  3.将df1和df2基于com_id,cust_seg join在一起
  异常值 : (x > mean+3*std) or (x < mean-3*std) 
"""
def get_mon_order_except():
    try:
        co_cust=get_valid_co_cust(spark).select("cust_id","cust_seg","com_id")

        # -----------------------获取co_co_01
        co_co_01 = get_co_co_01(spark,scope=[1,1],filter="month") \
            .select("qty_sum", "amt_sum", "cust_id")
        #每个零售户的订货总量 总订货额
        qty_amt_sum = co_co_01.groupBy("cust_id")\
            .agg(f.sum("qty_sum").alias("order_sum"), f.sum("amt_sum").alias("amt_sum")) \
            .join(co_cust, "cust_id")


        # -----零售店单月订购总量异常
        try:
            order_sum=qty_amt_sum.select("com_id","cust_seg","cust_id","order_sum")

            cols={
                  "value":"order_sum",
                  "abnormal":"sum_abno_month",
                  "mean_plus_3std":"grade_sum_plus3",
                  "mean_minus_3std":"grade_sum_minu3",
                  "mean":"grade_sum"
                  }
            values=list(cols.values())
            values.remove(cols["value"])

            print(f"{str(dt.now())}  零售店单月订购总量异常")
            result=is_except(order_sum,cols,["com_id","cust_seg"])
            result.foreachPartition(lambda x:write_hbase1(x,values,hbase))
        except Exception as e:
            tb.print_exc()

        #-----零售店单月订货条均价异常
        try:
            avg_price=qty_amt_sum.withColumn("avg_price", col("amt_sum")/col("order_sum"))\
                     .select("com_id","cust_seg","cust_id","avg_price")

            cols = {
                    "value":"avg_price",
                    "abnormal": "price_abno_month",
                    "mean_plus_3std": "grade_price_plus3",
                    "mean_minus_3std": "grade_price_minu3",
                    "mean": "grade_price"
                    }
            values = list(cols.values())
            values.remove(cols["value"])

            print(f"{str(dt.now())}  零售店单月订货条均价异常")
            result = is_except(avg_price, cols,["com_id","cust_seg"])
            result.foreachPartition(lambda x: write_hbase1(x, values, hbase))
        except Exception as e:
            tb.print_exc()


    except Exception as e:
        tb.print_exc()

# get_mon_order_except()





def get_last_order_except():
    try:

        co_co_01 = get_co_co_01(spark,[0,30])\
                           .select("qty_sum", "amt_sum", "cust_id", "born_date")

        co_cust = get_valid_co_cust(spark).select("cust_id", "cust_seg", "com_id")

        #零售户上一次订货量  取零售户日期最大的订单
        qty_amt_sum = co_co_01.groupBy("cust_id").agg(f.max("born_date").alias("born_date")) \
            .join(co_co_01, ["cust_id", "born_date"]) \
            .join(co_cust, "cust_id")


        #-----零售店单次订购总量异常
         #1.如果上一次为退货，即订单量为负
        try:
            order_sum = qty_amt_sum.select("com_id", "cust_seg", "cust_id", "qty_sum")

            cols = {
                    "value":"qty_sum",
                    "abnormal": "sum_abno_time",
                    "mean_plus_3std": "last_sum_plus3",
                    "mean_minus_3std": "last_sum_minu3",
                    "mean": "last_sum_mean"
                    }
            values = list(cols.values())


            print(f"{str(dt.now())}  零售店上次订购总量异常")
            result = is_except(order_sum, cols, ["com_id", "cust_seg"])
            result.foreachPartition(lambda x:write_hbase1(x,values,hbase))
        except Exception:
            tb.print_exc()

        # -----零售店单次订货条均价异常
        try:
            avg_price = qty_amt_sum.withColumn("avg_price", col("amt_sum")/col("qty_sum")) \
                .select("com_id", "cust_seg", "cust_id", "avg_price")

            cols = {
                    "value":"avg_price",
                    "abnormal": "avg_abno_time",
                    "mean_plus_3std": "last_avg_plus3",
                    "mean_minus_3std": "last_avg_minu3",
                    "mean": "last_avg_mean"
                    }
            values = list(cols.values())


            print(f"{str(dt.now())}  零售店上次订货条均价异常")
            result = is_except(avg_price, cols, ["com_id", "cust_seg"])
            result.foreachPartition(lambda x:write_hbase1(x,values,hbase))
        except Exception:
            tb.print_exc()
    except Exception:
        tb.print_exc()
# get_last_order_except()





def get_mon_item_except():
    try:
        # -----------------------获取co_co_line
        co_co_line=get_co_co_line(spark,scope=[1,1],filter="month")\
                                  .select("cust_id","item_id","qty_ord","price")

        co_cust = get_valid_co_cust(spark).select("cust_id","cust_seg","com_id")

        plm_item=get_plm_item(spark).select("item_id","yieldly_type")

        #零售户所定烟的总数目
        item_num=co_co_line.groupBy("cust_id").agg(f.sum("qty_ord").alias("item_num"))


        # -----零售店单月省外烟订购异常
        try:
            print(f"{str(dt.now())}  零售店单月省外烟订购异常")

            line_plm=co_co_line.join(plm_item,"item_id")
            #零售户所定省外烟的数目
            out_prov_num=line_plm.where(col("yieldly_type")=="1")\
                                 .groupBy("cust_id")\
                                 .agg(f.sum("qty_ord").alias("out_prov_num"))

            #每个零售户省外烟占比
            out_prov_ratio=out_prov_num.join(item_num,"cust_id")\
                     .withColumn("out_prov_ratio",col("out_prov_num")/col("item_num"))\
                     .join(co_cust,"cust_id") \
                     .select("com_id", "cust_seg", "cust_id", "out_prov_ratio")

            cols={
                  "value":"out_prov_ratio",
                  "abnormal":"out_prov_abno_month",
                  "mean_plus_3std":"grade_out_prov_plus3",
                  "mean_minus_3std":"grade_out_prov_minu3",
                  "mean":"grade_out_prov"
                 }
            values = list(cols.values())
            values.remove(cols["value"])


            result=is_except(out_prov_ratio,cols,["com_id","cust_seg"])
            result.foreachPartition(lambda x:write_hbase1(x,values,hbase))
        except Exception as e:
            tb.print_exc()


        #-----零售店单月高价烟订购异常   字段还未命名
        try:
            print(f"{str(dt.now())}  零售店单月高价烟订购异常")
            #每个零售户高价烟的数量
            high_price_num = co_co_line.where(col("price") >= 500) \
                .groupBy("cust_id") \
                .agg(f.sum("qty_ord").alias("high_price_num"))
            #每个零售户高价烟数量/总量
            high_price_ratio = item_num.join(high_price_num, "cust_id") \
                .withColumn("high_price_ratio", col("high_price_num")/col("item_num")) \
                .join(co_cust, "cust_id") \
                .select("com_id", "cust_seg", "cust_id", "high_price_ratio")

            cols = {
                    "value":"high_price_ratio",
                    "abnormal": "high_abno_month",
                    "mean_plus_3std": "high_ratio_plus3",
                    "mean_minus_3std": "high_ratio_minu3",
                    "mean": "high_ratio_mean"
                    }
            values = list(cols.values())
            values.remove(cols["value"])



            result = is_except(high_price_ratio,cols,["com_id","cust_seg"])
            result.foreachPartition(lambda x: write_hbase1(x, values, hbase))
        except Exception as e:
            tb.print_exc()

    except Exception as e:
        tb.print_exc()

# get_mon_item_except()





def get_last_item_except():
    try:
        # -----------------------获取co_co_line
        co_co_line = get_co_co_line(spark,scope=[0,30]) \
                    .select("cust_id", "item_id", "qty_ord", "price","born_date")

        co_cust = get_valid_co_cust(spark).select("cust_id", "cust_seg", "com_id")

        plm_item = get_plm_item(spark).select("item_id", "yieldly_type")

        # 零售户最近一次所定的烟
        last_time = co_co_line.groupBy("cust_id").agg(f.max("born_date").alias("born_date")) \
            .join(co_co_line, ["cust_id", "born_date"])

        # 零售户最近一次所定烟总量
        item_num = last_time.groupBy("cust_id").agg(f.sum("qty_ord").alias("item_num"))

        # -----零售店上次省外烟订购异常
        print(f"{str(dt.now())}  零售店上次省外烟订购异常")
        try:
            line_plm = last_time.join(plm_item, "item_id")
            # 零售户所定省外烟的数目
            out_prov_num = line_plm.where(col("yieldly_type") == "1") \
                .groupBy("cust_id") \
                .agg(f.sum("qty_ord").alias("out_prov_num"))

            # 每个零售户省外烟占比
            out_prov_ratio = out_prov_num.join(item_num, "cust_id") \
                .withColumn("last_out_prov_ratio", col("out_prov_num")/col("item_num")) \
                .join(co_cust, "cust_id") \
                .select("com_id", "cust_seg", "cust_id", "last_out_prov_ratio")

            cols = {
                "value": "last_out_prov_ratio",
                "abnormal": "out_prov_abno_time",
                "mean_plus_3std": "last_out_ratio_plus3",
                "mean_minus_3std": "last_out_ratio_minu3",
                "mean": "last_out_ratio_mean"
            }
            values = list(cols.values())

            result = is_except(out_prov_ratio, cols, ["com_id", "cust_seg"])
            result.foreachPartition(lambda x: write_hbase1(x, values, hbase))
        except Exception as e:
            tb.print_exc()

        # -----零售店上次高价烟订购异常   字段还未命名
        try:
            print(f"{str(dt.now())}  零售店上次高价烟订购异常")
            # 每个零售户高价烟的数量
            high_price_num = last_time.where(col("price") >= 500) \
                .groupBy("cust_id") \
                .agg(f.sum("qty_ord").alias("high_price_num"))
            # 每个零售户高价烟数量/总量
            high_price_ratio = item_num.join(high_price_num, "cust_id") \
                .withColumn("last_high_price_ratio", col("high_price_num")/col("item_num")) \
                .join(co_cust, "cust_id") \
                .select("com_id", "cust_seg", "cust_id", "last_high_price_ratio")

            cols = {
                "value": "last_high_price_ratio",
                "abnormal": "high_abno_time",
                "mean_plus_3std": "last_high_ratio_plus3",
                "mean_minus_3std": "last_high_ratio_minu3",
                "mean": "last_high_ratio_mean"
            }
            values = list(cols.values())

            result = is_except(high_price_ratio, cols, ["com_id", "cust_seg"])
            result.foreachPartition(lambda x: write_hbase1(x, values, hbase))
        except Exception as e:
            tb.print_exc()

    except Exception as e:
        tb.print_exc()
# get_last_item_except()





#-----零售店订购烟品规异常
"""
1.通过co_co_line计算每个零售户所定各类烟的数量cust_item_sum
2.通过窗口函数计算每个零售户订购量前三的烟rank3
3.计算每个零售户周边订购量前三的烟one_km_rank3
   3.1 零售户cust_id1周边包含cust_id0这些零售户,再和cust_item_sum表 join获取cust_id0这些零售户的每类烟的订购量
   3.2 根据cust_id1和item_name 分组聚合，然后取前三
四.取交集，长度为0，异常
"""
def get_around_class_except():
    try:
        print(f"{str(dt.now())}  零售店订购烟品规异常")
        co_cust=get_valid_co_cust(spark).select("cust_id")

        co_co_line = get_co_co_line(spark,scope=[1,1],filter="month")\
                                  .select("cust_id","item_id","qty_ord")

        plm_item=get_plm_item(spark).select("item_id","item_name")


        # 1.每个零售户 每类烟 的数量
        cust_item_sum = co_co_line.join(plm_item, "item_id") \
            .groupBy("cust_id", "item_name") \
            .agg(f.sum("qty_ord").alias("cust_item_sum"))


        #2.每个零售户订购量前三的烟
        win = Window.partitionBy("cust_id").orderBy(col("cust_item_sum").desc())
        rank3 = cust_item_sum.withColumn("rank", f.row_number().over(win)) \
            .where(col("rank") <= 3) \
            .groupBy("cust_id") \
            .agg(f.collect_list("item_name").alias("items"))


        win = Window.partitionBy("cust_id1").orderBy(col("one_km_item_sum").desc())
        #每个零售户cust_id1 周边的零售户cust_id0
        around_cust = get_around_cust(spark, 1).select("cust_id1","cust_id0")
        """
          零售户cust_id1周边包含cust_id0这些零售户 
        1.第一个join，计算每个零售户cust_id1一公里内有哪些零售户cust_id0
        2.第二个join，一公里内的cust_id0这些零售户所定各类烟的数量   cust_id0 与 cust_item_sum的cust_id
        3.根据cust_id1 item_name 计算一公里内各类烟的数量
        """
        #3.每个零售户一公里内所定烟的前三
        one_km_rank3 = around_cust.join(cust_item_sum, col("cust_id0") == col("cust_id")) \
            .select("cust_id1", "item_name", "cust_item_sum") \
            .groupBy("cust_id1", "item_name") \
            .agg(f.sum("cust_item_sum").alias("one_km_item_sum")) \
            .withColumn("rank", f.row_number().over(win)) \
            .where(col("rank") <= 3) \
            .groupBy("cust_id1") \
            .agg(f.collect_list("item_name").alias("one_km_items"))



        colName=["regulation_abno","ciga_top3_last_month","ciga_top3_km"]
        #四.求交集 长度为0，异常
        rank3.join(one_km_rank3, col("cust_id") == col("cust_id1")) \
            .where(f.size(f.array_intersect(col("items"), col("one_km_items"))) == 0) \
            .withColumn(colName[0],f.lit(1))\
            .withColumnRenamed("items",colName[1])\
            .withColumnRenamed("one_km_items",colName[2])\
            .join(co_cust,"cust_id")\
            .foreachPartition(lambda x:write_hbase1(x,colName,hbase))
    except Exception as e:
        tb.print_exc()

# get_around_class_except()





#-----零售店上月订货条均价与所在一公里范围内消费水平（餐饮、酒店，租金）比值高于/低于全市零售店均值的B%以上
def get_avg_cons_level():
    try:
        co_cust=get_valid_co_cust(spark).select("cust_id")
        print(f"{str(dt.now())}  零售店订货条均价/消费水平")
        consume_level_df=get_consume_level(spark).select("cust_id","consume_level")

         # -----------------------获取co_co_01
        co_co_01=get_co_co_01(spark,scope=[1,1],filter="month") \
                .select("com_id", "qty_sum", "amt_sum", "cust_id")

        #每个零售户的订货总量 总订货额
        qty_amt_sum = co_co_01.groupBy("com_id","cust_id")\
            .agg(f.sum("qty_sum").alias("order_sum"), f.sum("amt_sum").alias("amt_sum"))


        #每个零售户的订货条均价
        avg_price=qty_amt_sum.withColumn("avg_price", col("amt_sum")/col("order_sum"))\
                   .select("com_id","cust_id","avg_price")

        avg_consume_level=avg_price.join(consume_level_df,"cust_id")\
                 .withColumn("retail_price_cons",col("avg_price")/col("consume_level"))

        cols={
              "value":"retail_price_cons",
              "abnormal":"price_cons",
              "mean_plus_3std":"price_std_plus3",
              "mean_minus_3std":"price_std_minu3",
              "mean":"city_price_cons"
              }
        values=list(cols.values())


        is_except(avg_consume_level,cols,["com_id"])\
                        .join(co_cust,"cust_id")\
                        .foreachPartition(lambda x:write_hbase1(x,values,hbase))
    except Exception:
        tb.print_exc()
# get_avg_cons_level()





#-----零售店上月高价烟订货比例与所在地一公里范围内消费水平（餐饮、酒店、租金）高于/低于全市零售店均值的B%以上
def get_high_cons_level():
    try:
        co_cust = get_valid_co_cust(spark).select("cust_id")
        print(f"{str(dt.now())}  零售店高价烟比例/消费水平")
        # 消费水平
        consume_level_df = get_consume_level(spark).select("cust_id", "consume_level")

        # -----------------------获取co_co_line
        co_co_line=get_co_co_line(spark,scope=[1,1],filter="month") \
            .select("cust_id","qty_ord","price","com_id")

        #零售户所定烟的总数目
        item_num=co_co_line.groupBy("com_id","cust_id").agg(f.sum("qty_ord").alias("item_num"))


        #每个零售户高价烟的数量
        high_price_num = co_co_line.where(col("price") >= 500) \
            .groupBy("cust_id") \
            .agg(f.sum("qty_ord").alias("high_price_num"))
        #每个零售户高价烟比例
        high_price_ratio = item_num.join(high_price_num, "cust_id") \
            .withColumn("high_price_ratio", col("high_price_num")/col("item_num")) \
            .select("com_id", "cust_id", "high_price_ratio")


        high_consume_level=high_price_ratio.join(consume_level_df,"cust_id")\
                        .withColumn("retail_high_cons",col("high_price_ratio")/col("consume_level"))


        cols={
              "value":"retail_high_cons",
              "abnormal":"high_cons",
              "mean_plus_3std":"high_std_plus3",
              "mean_minus_3std":"high_std_minu3",
              "mean":"city_high_cons"
              }
        values=list(cols.values())

        is_except(high_consume_level,cols,["com_id"])\
                 .join(co_cust,"cust_id")\
                 .foreachPartition(lambda x:write_hbase1(x,values,hbase))
    except Exception:
        tb.print_exc()

# get_high_cons_level()



print(f"{str(dt.now())}  end")








