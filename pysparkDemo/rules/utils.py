#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/4/8 16:39
import math
from pyspark.sql.types import FloatType,IntegerType
from pyspark.sql.functions import udf,date_trunc,datediff,col
from pyspark.sql import functions as f
from pyspark.sql import SparkSession
#---------------------------------------udf---------------------------------------
def period(x,y):
    try:
        x=float(x)
        y=float(y)
        return round((x-y)/y,6)
    except Exception as e:
        print(e.args)
        print(x,y)
period_udf = udf(period,FloatType())


def divider(x,y):
    try:
        return round(float(x)/float(y),6)
    except Exception as e:
        print(e.args)
        print(x,y)
divider_udf=udf(divider,FloatType())


def month_diff(year,month,today_year,today_month):
    db=today_year-year
    month_diff=(today_month+db*12)-month
    return month_diff
month_diff_udf=udf(month_diff)


def week_diff0(year, month, week, today_year, today_month, today_week):
    db = today_year - year
    # 因为2018-12-31与2019-01-01 的 weekofyear都是1，db要减去1
    if week == 1 and month != today_month:
        db -= 1
    week_diff = (today_week + 52 * db) - week
    return week_diff

week_diff_udf=udf(week_diff0)


def week_diff(end,start):
    """
         计算两个日期的相隔周数
    :param end:
    :param start:
    :return:
    """
    # date_trunc("week",date) 返回 所属周的周一的日期
    return datediff(date_trunc("week", col(end)), date_trunc("week", col(start))) / 7



def grade_diff(max1, min1, max2, min2):
    """
         一段时间内某零售户 档位更改差值
         1.分别求grade_frm,grade_to的最大，最小
         2.用这四个数中的最大-最小
    :param max1:    grade_frm的最大值
    :param min1:    grade_frm的最小值
    :param max2:    grade_to的最大值
    :param min2:    grade_to的最小值
    :return:
    """
    try:
        l = sorted([int(max1), int(min1), int(max2), int(min2)])
        diff = l[3] - l[0]
        return diff
    except Exception as e:
        print(e.args)
grade_diff_udf = udf(grade_diff,IntegerType())

def box_plots_filter(grade_diff,percent_25,percent_75):
    """
           箱形图/箱线图
    :param grade_diff:
    :param percent_25:
    :param percent_75:
    :return:
    """
    iqr=percent_75-percent_25
    upper=percent_75+1.5*iqr
    lower=percent_25-1.5*iqr
    if grade_diff>upper:
        return 1
    elif grade_diff<lower:
        return 0
    else:
        return -1
box_plots_filter_udf=udf(box_plots_filter)



#计算一个经纬度为中心，上下相差1km的纬度范围 左右相差1km的经度范围
#1km
scope=1
#cos(弧度)
lng_r=udf(lambda lng,lat:lng+scope/(111.11*math.cos(math.radians(lat))),FloatType())
lng_l=udf(lambda lng,lat:lng-scope/(111.11*math.cos(math.radians(lat))),FloatType())
lat_u=udf(lambda lat:lat+scope/111.11,FloatType())
lat_d=udf(lambda lat:lat-scope/111.11,FloatType())




def fill_0(cust_id):
    if len(cust_id)==10:
        cust_id=f"0{cust_id}"
    return cust_id
fill_0_udf=udf(fill_0)



def consume_level(rent,food,hotel,rent_25,food_25,hotel_25):
    # 判断rent food hotel是否全为空，
    # 是 返回空
    # 否 一个或两个为空，填补对应的25%分位的值，然后相加
    if rent==None and  food==None and hotel==None:
        return None
    else:
        if rent==None:
            rent=rent_25
        if food==None:
            food=food_25
        if hotel==None:
            hotel=hotel_25
        return rent+food+hotel
consume_level_udf=udf(consume_level,FloatType())


def abnormal_range(value,upper,lower):
    try:
        if value>upper:
            return 1
        elif value < lower:
            return 0
        else:
            return -1
    except Exception:
        return -1

abnormal_range_udf=udf(abnormal_range)



add=udf(lambda x,y:float(x)+float(y),FloatType())



def is_except(df,cols:dict):
    """

    :param df:  cust_id com_id cust_seg value
    :param cols:
    """
    abnormal=cols["abnormal"]
    mean_plus_3std=cols["mean_plus_3std"]
    mean_minus_3std=cols["mean_minus_3std"]
    mean=cols["mean"]

    #按照 市 档位分组 求vlaue的均值和标准差
    mean_3std = df.groupBy("com_id", "cust_seg") \
        .agg(f.mean("value").alias("mean"), (f.stddev_pop("value") * 3).alias("+3std"),
             (f.stddev_pop("value") * (-3)).alias("-3std")) \
        .withColumn("mean+3std", add(col("mean"), col("+3std"))) \
        .withColumn("mean-3std", add(col("mean"), col("-3std")))

    result = df.join(mean_3std, ["com_id", "cust_seg"]) \
        .withColumn(abnormal, abnormal_range_udf(col("value"), col("mean+3std"), col("mean-3std"))) \
        .withColumnRenamed("mean+3std", mean_plus_3std) \
        .withColumnRenamed("mean-3std", mean_minus_3std) \
        .withColumnRenamed("mean",mean)\
        .where(col(abnormal) >= 0)

    return result


#---------------------------------数据集--------------------------------

def get_co_cust(spark):
    # -----------------co_cust 零售客户信息表   全量更新  选取dt最新的数据 并过滤掉无效的cust_id，即status为04(无效)
    co_cust=spark.sql("select * from DB2_DB2INST1_CO_CUST where dt=(select max(dt) from DB2_DB2INST1_CO_CUST) and status !=04")
    return co_cust
def get_crm_cust(spark):
    # ----------------crm_cust 零售客户信息表 为co_cust表的补充  全量更新  选取dt最新的数据
    crm_cust=spark.sql("select * from DB2_DB2INST1_CRM_CUST where dt=(select max(dt) from DB2_DB2INST1_CRM_CUST)") \
                    .withColumnRenamed("crm_longitude", "longitude") \
                    .withColumnRenamed("crm_latitude", "latitude")
    return crm_cust
def get_co_co_01(spark):
    #获取co_co_01        unique_kind: 90 退货  10 普通订单    pmt_status:  0 未付款  1 收款完成
    co_co_01 = spark.sql("select  * from DB2_DB2INST1_CO_CO_01 where (unique_kind = 90 and pmt_status=0) or (unique_kind=10 and pmt_status=1)") \
        .withColumn("born_date", f.to_date("born_date", "yyyyMMdd"))\
        .withColumn("today",f.current_date())\
        .withColumn("qty_sum",col("qty_sum").cast("float"))\
        .withColumn("amt_sum",col("amt_sum").cast("float"))
    return co_co_01

def get_co_co_line(spark):
    co_co_line = spark.sql(
        "select * from DB2_DB2INST1_CO_CO_LINE") \
        .withColumn("born_date", f.to_date("born_date", "yyyyMMdd")) \
        .withColumn("today", f.current_date()) \
        .withColumn("qty_ord", col("qty_ord").cast("float")) \
        .withColumn("price", col("price").cast("float"))
    return co_co_line

def get_crm_cust_log(spark):
    #crm_cust_log 变更记录表
    # change_type:  CO_CUST.STATUS  状态变更
    #               CO_CUST.CUST_SEG  档位变更
    crm_cust_log = spark.sql("select cust_id,change_type,change_frm,change_to,audit_date from DB2_DB2INST1_CRM_CUST_LOG") \
        .withColumn("audit_date", f.to_date("audit_date", "yyyyMMdd")) \
        .withColumn("day_diff",f.datediff(f.current_date(), col("audit_date")))
    return crm_cust_log

def get_co_debit_acc(spark):
    # 扣款账号信息表 全量更新
    co_debit_acc = spark.sql("select * from DB2_DB2INST1_CO_DEBIT_ACC "
                             "where dt=(select max(dt) from DB2_DB2INST1_CO_DEBIT_ACC)")
    return co_debit_acc

def get_plm_item(spark):
    #卷烟信息
    # ------------------获取plm_item
    plm_item = spark.sql("select * from DB2_DB2INST1_PLM_ITEM where dt=(select max(dt) from DB2_DB2INST1_PLM_ITEM)")
    return plm_item

def get_coordinate(spark,hdfs_path):
    # 餐厅、交通、商城、娱乐场馆等经纬度
    coordinate = spark.read.csv(header=True, path=hdfs_path) \
        .withColumn("lng", col("longitude").cast("float")) \
        .withColumn("lat", col("latitude").cast("float")) \
        .select("lng", "lat", "types")
    return coordinate

def get_area(spark,path):
    area_code = spark.read.csv(path=path, header=True)
    return area_code


def get_cust_lng_lat(spark):
    dir = "/user/entrobus/zhangzy/long_lat/"
    files = {"株洲市": "株洲市零售户地址+经纬度+区域编码.csv",
             "邵阳市": "邵阳市零售户地址+经纬度+区域编码.csv",
             "岳阳市": "岳阳市零售户地址+经纬度+区域编码.csv"}
    cities = list(files.keys())
    dfs = []
    for city in cities:
        path = dir + files[city]
        df = spark.read.csv(header=True, path=path) \
            .withColumn("city", f.lit(city))\
            .withColumn("longitude", col("longitude").cast("float")) \
            .withColumn("latitude", col("latitude").cast("float")) \
            .dropna(how="any", subset=["latitude", "latitude"])
            # .withColumn("cust_id", fill_0_udf(col("cust_id"))) \
        dfs.append(df)

    df = dfs[0]
    for i in range(1, len(dfs)):
        df = df.unionByName(dfs[i])
    return df


if __name__=="__main__":
    print(divider("10","20"))