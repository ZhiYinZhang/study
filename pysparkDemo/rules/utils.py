#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/4/8 16:39
import math
from pyspark.sql.types import FloatType
from pyspark.sql.functions import udf,date_trunc,datediff,col
def period(x,y):
    try:
        x=float(x)
        y=float(y)
        return (x-y)/y
    except Exception as e:
        print(e.args)
        print(x,y)
period_udf = udf(period,FloatType())


def divider(x,y):
    try:
        return x/y
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
    # date_trunc("week",date) 返回 所属周的周一的日期
    return datediff(date_trunc("week", col(end)), date_trunc("week", col(start))) / 7

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



if __name__=="__main__":
    print(month_diff(18, 3 , 18, 4))
    # print(period(str(95.47),str(0.0000)))