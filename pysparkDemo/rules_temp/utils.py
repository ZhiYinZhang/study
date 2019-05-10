#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/4/8 16:39
import math
import traceback as tb
from datetime import datetime as dt
from pyspark.sql.types import FloatType,IntegerType
from pyspark.sql.functions import udf,date_trunc,datediff,col
from pyspark.sql import functions as f
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



#计算一个经纬度为中心，上下相差scope km的纬度范围 左右相差scope km的经度范围
#cos(弧度)
lng_r=udf(lambda lng,lat,scope:lng+scope/(111.11*math.cos(math.radians(lat))),FloatType())
lng_l=udf(lambda lng,lat,scope:lng-scope/(111.11*math.cos(math.radians(lat))),FloatType())
lat_u=udf(lambda lat,scope:lat+scope/111.11,FloatType())
lat_d=udf(lambda lat,scope:lat-scope/111.11,FloatType())




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



def is_except(df,cols:dict,groupBy:list):
    """

    :param df: 包含的columns:cust_id com_id cust_seg value
    :param cols:
    :param groupBy: list
    """
    value=cols["value"]
    abnormal=cols["abnormal"]
    mean_plus_3std=cols["mean_plus_3std"]
    mean_minus_3std=cols["mean_minus_3std"]
    mean=cols["mean"]

    #按照 市 档位分组 求vlaue的均值和标准差
    mean_3std = df.groupBy(groupBy) \
        .agg(f.mean(value).alias("mean"), (f.stddev_pop(value) * 3).alias("+3std"),
             (f.stddev_pop(value) * (-3)).alias("-3std")) \
        .withColumn("mean+3std", col("mean")+col("+3std")) \
        .withColumn("mean-3std", col("mean")+col("-3std"))

    result = df.join(mean_3std, groupBy) \
        .withColumn(abnormal, abnormal_range_udf(col(value), col("mean+3std"), col("mean-3std"))) \
        .withColumnRenamed("mean+3std", mean_plus_3std) \
        .withColumnRenamed("mean-3std", mean_minus_3std) \
        .withColumnRenamed("mean",mean)\
        .where(col(abnormal) >= 0)

    return result

# 零售户周边(1km)消费水平
def get_consume_level(spark):
    """
    :param spark: SparkSession
    :return:
    """
    # 租金 餐饮 酒店信息
    rent_food_hotel = "/user/entrobus/zhangzy/rent_food_hotel/"
    poi = {"rent": "fang_zu0326 xin.csv",
           "food": "food_0326.csv",
           "hotel": "shaoyang_hotel_shop_list0326.csv"}

    try:
        cust_lng_lat = get_cust_lng_lat(spark).select("city", "cust_id", "longitude", "latitude") \
            .withColumn("scope",f.lit(1))\
            .withColumn("lng_l", lng_l(col("longitude"), col("latitude"),col("scope"))) \
            .withColumn("lng_r", lng_r(col("longitude"), col("latitude"),col("scope"))) \
            .withColumn("lat_d", lat_d(col("latitude"),col("scope"))) \
            .withColumn("lat_u", lat_u(col("latitude"),col("scope"))) \
            .drop("longitude", "latitude")

        # -------------租金
        print(f"{str(dt.now())}  rent")
        path = rent_food_hotel + poi["rent"]
        rent = spark.read.csv(header=True, path=path) \
            .withColumn("lng", col("longitude").cast("float")) \
            .withColumn("lat", col("latitude").cast("float")) \
            .select("lng", "lat", "price_1square/(元/平米)")
        # 零售户一公里的每平米租金
        city_rent = rent.join(cust_lng_lat, (col("lng") >= col("lng_l")) & (col("lng") <= col("lng_r")) & (
                col("lat") >= col("lat_d")) & (col("lat") <= col("lat_u")))
        # 每个零售户一公里范围内的每平米租金的均值
        city_rent_avg = city_rent.groupBy("city", "cust_id").agg(f.mean("price_1square/(元/平米)").alias("rent_avg"))

        # 各市25%分位值
        city_rent_avg.registerTempTable("city_rent_avg")
        city_rent_avg = spark.sql(
            "select city,percentile_approx(rent_avg,0.25) as rent_25 from city_rent_avg group by city") \
            .join(city_rent_avg, "city")

        # --------------餐饮
        print(f"{str(dt.now())}  food")
        path = rent_food_hotel + poi["food"]
        food = spark.read.csv(header=True, path=path) \
            .withColumn("lng", col("jingdu").cast("float")) \
            .withColumn("lat", col("weidu").cast("float")) \
            .select("lng", "lat", "mean_prices2")
        city_food = food.join(cust_lng_lat, (col("lng") >= col("lng_l")) & (col("lng") <= col("lng_r")) & (
                col("lat") >= col("lat_d")) & (col("lat") <= col("lat_u")))
        # 每个零售户一公里范围内的餐饮价格的均值
        city_food_avg = city_food.groupBy("city", "cust_id").agg(f.mean("mean_prices2").alias("food_avg"))

        # 各市25%分位值
        city_food_avg.registerTempTable("city_food_avg")
        city_food_avg = spark.sql(
            "select city,percentile_approx(food_avg,0.25) as food_25 from city_food_avg group by city") \
            .join(city_food_avg, "city")

        # --------------酒店
        print(f"{str(dt.now())}  hotel")
        path = rent_food_hotel + poi["hotel"]
        hotel = spark.read.csv(header=True, path=path) \
            .withColumn("lng", col("jingdu").cast("float")) \
            .withColumn("lat", col("weidu").cast("float")) \
            .select("lng", "lat", "price_new")
        city_hotel = hotel.join(cust_lng_lat, (col("lng") >= col("lng_l")) & (col("lng") <= col("lng_r")) & (
                col("lat") >= col("lat_d")) & (col("lat") <= col("lat_u")))
        # 每个零售户一公里范围内的餐饮价格的均值
        city_hotel_avg = city_hotel.groupBy("city", "cust_id").agg(f.mean("price_new").alias("hotel_avg"))

        # 各市25%分位值
        city_hotel_avg.registerTempTable("city_hotel_avg")
        city_hotel_avg = spark.sql(
            "select city,percentile_approx(hotel_avg,0.25) as hotel_25 from city_hotel_avg group by city") \
            .join(city_hotel_avg, "city")

        print(f"{str(dt.now())}   consume level")
        consume_level_df = city_rent_avg.join(city_food_avg, ["city", "cust_id"],"outer") \
            .join(city_hotel_avg, ["city", "cust_id"],"outer") \
            .withColumn("cust_id", fill_0_udf(col("cust_id"))) \
            .fillna(0, ["rent_25", "food_25", "hotel_25"]) \
            .withColumn("consume_level",consume_level_udf(col("rent_avg"), col("food_avg"), col("hotel_avg"), col("rent_25"),col("food_25"), col("hotel_25")))

        return consume_level_df
    except Exception:
        tb.print_exc()
#---------------------------------数据集--------------------------------

def get_co_cust(spark):
    # -----------------co_cust 零售客户信息表   全量更新  选取dt最新的数据
    co_cust=spark.sql("select * from DB2_DB2INST1_CO_CUST where dt=(select max(dt) from DB2_DB2INST1_CO_CUST) and status !=04")
    return co_cust
def get_valid_co_cust(spark):
    #获取有效的零售户
    co_cust=spark.sql("select * from DB2_DB2INST1_CO_CUST where dt=(select max(dt) from DB2_DB2INST1_CO_CUST)")\
                 .where(col("status").rlike("(01)|(02)"))
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
        .withColumn("day_diff",f.datediff(f.current_date(), col("audit_date"))) \
        .withColumn("change_frm", f.regexp_replace(col("change_frm"), "(zz)|(ZZ)", "31")) \
        .withColumn("change_to", f.regexp_replace(col("change_to"), "(zz)|(ZZ)", "31"))
    return crm_cust_log

def get_co_debit_acc(spark):
    # 扣款账号信息表 全量更新
    co_debit_acc = spark.sql("select * from DB2_DB2INST1_CO_DEBIT_ACC "
                             "where dt=(select max(dt) from DB2_DB2INST1_CO_DEBIT_ACC) and status=1")
    return co_debit_acc

def get_plm_item(spark):
    #卷烟信息
    # ------------------获取plm_item
    plm_item = spark.sql("select * from DB2_DB2INST1_PLM_ITEM where dt=(select max(dt) from DB2_DB2INST1_PLM_ITEM)")
    return plm_item

def get_coordinate(spark):
    path = "/user/entrobus/zhangzy/dataset/coordinate.csv"
    # 餐厅、交通、商城、娱乐场馆等经纬度
    coordinate = spark.read.csv(header=True, path=path) \
        .withColumn("lng", col("longitude").cast("float")) \
        .withColumn("lat", col("latitude").cast("float"))
    return coordinate

def get_area(spark):
    path = "/user/entrobus/zhangzy/区县名称与sale_center_id匹配关系0410+区域编码.csv"
    area_code = spark.read.csv(path=path, header=True) \
                .withColumnRenamed("城市", "city") \
                .withColumnRenamed("区", "county")
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
            # .withColumn("cust_id", fill_0_udf(col("cust_id"))) \#cust_id前面少个0
        dfs.append(df)

    df = dfs[0]
    for i in range(1, len(dfs)):
        df = df.unionByName(dfs[i])
    return df


def get_city_info(spark):
    dir = "/user/entrobus/zhangzy/gdp/"
    cities = {"邵阳市": "邵阳统计数据.csv",
              "岳阳市": "岳阳统计数据.csv"}
    dfs = []
    for city in cities.keys():
        path = dir + cities[city]
        city_gdp = spark.read.csv(path, header=True) \
            .withColumn("city", f.lit(city))
        if city == "邵阳市":
            city_gdp = city_gdp.withColumn("mtime", f.to_date("mtime", "yy-MMM"))
        elif city == "岳阳市":
            city_gdp = city_gdp.withColumn("mtime", f.to_date("mtime", "MMM-yy"))
        dfs.append(city_gdp)

    df = dfs[0]
    for i in range(1, len(dfs)):
        df = df.unionByName(dfs[i])

    return df
def get_city_ppl(spark):
    dir="/user/entrobus/zhangzy/population/"
    cities={"株洲市":"株洲人口数据.csv",
            "邵阳市":"邵阳人口数据.csv",
            "岳阳市":"岳阳人口数据.csv"}
    dfs=[]
    for city in cities.keys():
        path=dir+cities[city]
        population=spark.read.csv(path,header=True)\
                                .withColumn("总人口",f.trim(col("总人口")).cast("float"))\
                                .withColumn("city",f.lit(city))
        dfs.append(population)

    df=dfs[0]
    for i in range(1,len(dfs)):
        df=df.union(dfs[i])
    return df

if __name__=="__main__":
    print(divider("10","20"))