#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/4/8 16:39
import math
from pyspark.sql.types import FloatType,IntegerType
from pyspark.sql.functions import udf,date_trunc,datediff,col
from pyspark.sql import functions as f
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
    if grade_diff>upper or grade_diff<lower:
        return 1
    else:
        return 0
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
        if value>upper or value<lower:
            return 1
        else:
            return 0
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
        .where(col(abnormal) == 1)

    return result

if __name__=="__main__":
    print(divider("10","20"))