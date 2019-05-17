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
    #外部的零售户经纬度数据 部分cust_id前面缺了一个0
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

    result=df.join(mean_3std, groupBy)\
        .withColumn(abnormal,f.when(col(value)>col("mean+3std"),1)
                            .when(col(value)<col("mean-3std"),0)
                  )\
        .withColumnRenamed("mean+3std", mean_plus_3std) \
        .withColumnRenamed("mean-3std", mean_minus_3std) \
        .withColumnRenamed("mean", mean) \
        .dropna(subset=[abnormal])
    return result


def get_consume_level(spark):
    """
       零售户周边(1km)消费水平
    :param spark: SparkSession
    :return:
    """
    # 租金 餐饮 酒店信息
    rent_food_hotel = "/user/entrobus/zhangzy/rent_food_hotel/"
    poi = {"rent": "rent.csv",
           "food": "food.csv",
           "hotel": "hotel.csv"}

    try:
        cust_lng_lat = get_cust_lng_lat(spark).select("city", "cust_id", "longitude", "latitude") \
            .withColumn("scope", f.lit(1)) \
            .withColumn("lng_l", lng_l(col("longitude"), col("latitude"), col("scope"))) \
            .withColumn("lng_r", lng_r(col("longitude"), col("latitude"), col("scope"))) \
            .withColumn("lat_d", lat_d(col("latitude"), col("scope"))) \
            .withColumn("lat_u", lat_u(col("latitude"), col("scope"))) \
            .drop("longitude", "latitude")

        # -------------租金
        print(f"{str(dt.now())}  rent")
        path = rent_food_hotel + poi["rent"]
        # rent
        rent = spark.read.csv(header=True, path=path) \
            .withColumn("lng", col("longitude").cast("float")) \
            .withColumn("lat", col("latitude").cast("float")) \
            .select("lng", "lat", "price_1square/(元/平米)") \
            .dropna(subset=["lng", "lat", "price_1square/(元/平米)"])

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
        # food
        food = spark.read.csv(header=True, path=path) \
            .withColumn("lng", col("lng").cast("float")) \
            .withColumn("lat", col("lat").cast("float")) \
            .select("lng", "lat", "mean_prices") \
            .dropna(subset=["lng", "lat", "mean_prices"])

        city_food = food.join(cust_lng_lat, (col("lng") >= col("lng_l")) & (col("lng") <= col("lng_r")) & (
                col("lat") >= col("lat_d")) & (col("lat") <= col("lat_u")))
        # 每个零售户一公里范围内的餐饮价格的均值
        city_food_avg = city_food.groupBy("city", "cust_id").agg(f.mean("mean_prices").alias("food_avg"))

        # 各市25%分位值
        city_food_avg.registerTempTable("city_food_avg")
        city_food_avg = spark.sql(
            "select city,percentile_approx(food_avg,0.25) as food_25 from city_food_avg group by city") \
            .join(city_food_avg, "city")

        # --------------酒店
        print(f"{str(dt.now())}  hotel")
        path = rent_food_hotel + poi["hotel"]
        # hotel
        hotel = spark.read.csv(header=True, path=path) \
            .withColumn("lng", col("lng").cast("float")) \
            .withColumn("lat", col("lat").cast("float")) \
            .select("lng", "lat", "price") \
            .dropna(subset=["lng", "lat", "price"])

        city_hotel = hotel.join(cust_lng_lat, (col("lng") >= col("lng_l")) & (col("lng") <= col("lng_r")) & (
                col("lat") >= col("lat_d")) & (col("lat") <= col("lat_u")))
        # 每个零售户一公里范围内的餐饮价格的均值
        city_hotel_avg = city_hotel.groupBy("city", "cust_id").agg(f.mean("price").alias("hotel_avg"))

        # 各市25%分位值
        city_hotel_avg.registerTempTable("city_hotel_avg")
        city_hotel_avg = spark.sql(
            "select city,percentile_approx(hotel_avg,0.25) as hotel_25 from city_hotel_avg group by city") \
            .join(city_hotel_avg, "city")

        print(f"{str(dt.now())}   consume level")
        consume_level_df = city_rent_avg.join(city_food_avg, ["city", "cust_id"], "outer") \
            .join(city_hotel_avg, ["city", "cust_id"], "outer") \
            .withColumn("cust_id", fill_0_udf(col("cust_id"))) \
            .fillna(0, ["rent_25", "food_25", "hotel_25"]) \
            .withColumn("consume_level",
                        consume_level_udf(col("rent_avg"), col("food_avg"), col("hotel_avg"), col("rent_25"),
                                          col("food_25"), col("hotel_25")))

        return consume_level_df
    except Exception:
        tb.print_exc()


def around_except_grade(around_cust, value_df, cols:dict, grade=[3, 4, 5]):
    """
          零售户指标与周围零售户相比
    :param around_cust: 列:cust_id1,cust_id0  零售户cust_id1周边包含cust_id0这些零售户
    :param value_df:  cust_id value   零售户cust_id计算的值  条均价/订单额
    :param cols: {"value":value,"abnormal":""}
    :param grade: [3,4,5] 按照 3/4/5倍标准差分档
    :return:
    """
    value = cols["value"]
    abnormal = cols["abnormal"]

    plus_one_grade=cols["plus_one_grade"]
    minus_one_grade=cols["minus_one_grade"]
    plus_two_grade=cols["plus_two_grade"]
    minus_two_grade=cols["minus_two_grade"]
    plus_three_grade=cols["plus_three_grade"]
    minus_three_grade=cols["minus_three_grade"]

    #每个零售户周边均值 标准差
    mean_std = around_cust.join(value_df, col("cust_id0") == col("cust_id")) \
        .select("cust_id1", value) \
        .groupBy("cust_id1") \
        .agg(f.mean(value).alias("mean"), f.stddev_pop(col(value)).alias("stddev"))
    """
    预警C
        均值 + 3标准差 < 条均价 < 均值 + 4标准差   过高  11
        均值 - 4标准差 < 条均价 < 均值 - 3标准差   过低  10
    预警B
        均值 + 3标准差 < 条均价 < 均值 + 4标准差   过高  21
        均值 - 4标准差 < 条均价 < 均值 - 3标准差   过低  20
    预警A
        均值 + 3标准差 < 条均价 < 均值 + 4标准差   过高  31
        均值 - 4标准差 < 条均价 < 均值 - 3标准差   过低  30
    """
    except_cust = mean_std.join(value_df, col("cust_id1") == col("cust_id")) \
        .withColumn(plus_one_grade, col("mean") + col("stddev") * grade[0]) \
        .withColumn(minus_one_grade, col("mean") - col("stddev") * grade[0]) \
        .withColumn(plus_two_grade, col("mean") + col("stddev") * grade[1]) \
        .withColumn(minus_two_grade, col("mean") - col("stddev") * grade[1]) \
        .withColumn(plus_three_grade, col("mean") + col("stddev") * grade[2]) \
        .withColumn(minus_three_grade, col("mean") - col("stddev") * grade[2]) \
        .withColumn(abnormal,
                    f.when((col(value) > col(plus_one_grade)) & (col(value) < col(plus_two_grade)), "11")
                    .when((col(value) > col(minus_two_grade)) & (col(value) < col(minus_one_grade)), "10")
                    .when((col(value) > col(plus_two_grade)) & (col(value) < col(plus_three_grade)), "21")
                    .when((col(value) > col(minus_three_grade)) & (col(value) < col(minus_two_grade)), "20")
                    .when((col(value) > col(plus_three_grade)), "31")
                    .when((col(value) < col(minus_three_grade)), "30")
        ).dropna(subset=[abnormal])

    return except_cust


def except_grade(df, cols: dict, groupBy: list, grade: list):
    """
        零售户指标与 全市/全市同档位 零售户相比
    :param df: 包含的columns:cust_id com_id cust_seg value
    :param cols:
    :param groupBy: list   [city]/[city,cust_seg]
    :param grade: [3,4,5] 按照 3/4/5倍标准差分档
    """
    value = cols["value"]
    abnormal = cols["abnormal"]

    plus_one_grade = cols["plus_one_grade"]
    minus_one_grade = cols["minus_one_grade"]
    plus_two_grade = cols["plus_two_grade"]
    minus_two_grade = cols["minus_two_grade"]
    plus_three_grade = cols["plus_three_grade"]
    minus_three_grade = cols["minus_three_grade"]

    # 按照 [city]/[city,cust_seg]分组 求vlaue的均值和标准差
    mean_std = df.groupBy(groupBy) \
        .agg(f.mean(value).alias("mean"), f.stddev_pop(value).alias("stddev")) \
        .withColumn(plus_one_grade, col("mean") + col("stddev") * grade[0]) \
        .withColumn(minus_one_grade, col("mean") - col("stddev") * grade[0]) \
        .withColumn(plus_two_grade, col("mean") + col("stddev") * grade[1]) \
        .withColumn(minus_two_grade, col("mean") - col("stddev") * grade[1]) \
        .withColumn(plus_three_grade, col("mean") + col("stddev") * grade[2]) \
        .withColumn(minus_three_grade, col("mean") - col("stddev") * grade[2])

    result = df.join(mean_std, groupBy) \
        .withColumn(abnormal,
                    f.when((col(value) > col(plus_one_grade)) & (col(value) < col(plus_two_grade)), "11")
                    .when((col(value) > col(minus_two_grade)) & (col(value) < col(minus_one_grade)), "10")
                    .when((col(value) > col(plus_two_grade)) & (col(value) < col(plus_three_grade)), "21")
                    .when((col(value) > col(minus_three_grade)) & (col(value) < col(minus_two_grade)), "20")
                    .when((col(value) > col(plus_three_grade)), "31")
                    .when((col(value) < col(minus_three_grade)), "30")
                    ).dropna(subset=[abnormal])
    return result

def get_around_cust(spark,around:int):
    """
     获取零售户cust_id1 包含cust_id0这些零售户
    :param around: 周围 {around} km
    """
    #零售户经纬度
    cust_lng_lat=get_cust_lng_lat(spark).withColumn("cust_id0", fill_0_udf(col("cust_id")))\
                                        .select("cust_id0","longitude","latitude")
    #每个零售户  一公里的经度范围和纬度范围
    cust_lng_lat0 = cust_lng_lat.withColumn("scope",f.lit(around))\
                                .withColumn("lng_l", lng_l(col("longitude"), col("latitude"),col("scope"))) \
                                .withColumn("lng_r", lng_r(col("longitude"), col("latitude"),col("scope"))) \
                                .withColumn("lat_d", lat_d(col("latitude"),col("scope"))) \
                                .withColumn("lat_u", lat_u(col("latitude"),col("scope"))) \
                                .withColumnRenamed("cust_id0", "cust_id1") \
                                .select("cust_id1", "lng_l", "lng_r", "lat_d", "lat_u")
    #每个零售户cust_id1 周边有cust_id0这些零售户
    around_cust=cust_lng_lat.join(cust_lng_lat0,(col("longitude")>=col("lng_l")) & (col("longitude")<=col("lng_r")) & (col("latitude")>=col("lat_d")) & (col("latitude")<=col("lat_u")))\
           .select("cust_id1","cust_id0")
    return around_cust









#---------------------------------数据集--------------------------------

def get_co_cust(spark):
    # -----------------co_cust 零售客户信息表   全量更新  选取dt最新的数据
    co_cust=spark.sql("select * from DB2_DB2INST1_CO_CUST where dt=(select max(dt) from DB2_DB2INST1_CO_CUST) and status !=04") \
                      .where(col("com_id").isin(["011114305", "011114306", "011114302"]))
    return co_cust
def get_valid_co_cust(spark):
    #获取有效的零售户
    co_cust=spark.sql("select * from DB2_DB2INST1_CO_CUST where dt=(select max(dt) from DB2_DB2INST1_CO_CUST)")\
                 .where((col("status").rlike("(01)|(02)")) & (col("com_id").isin(["011114305","011114306","011114302"])))
    return co_cust


def get_crm_cust(spark):
    # ----------------crm_cust 零售客户信息表 为co_cust表的补充  全量更新  选取dt最新的数据
    crm_cust=spark.sql("select * from DB2_DB2INST1_CRM_CUST where dt=(select max(dt) from DB2_DB2INST1_CRM_CUST)") \
                    .where(col("com_id").isin(["011114305", "011114306", "011114302"]))\
                    .withColumnRenamed("crm_longitude", "longitude") \
                    .withColumnRenamed("crm_latitude", "latitude")
    return crm_cust
def get_co_co_01(spark,scope:list,filter="day"):
    """

    :param spark: SparkSession
    :param scope: [lower,upper]  日期过滤范围
    :param filter: 过滤类别  "day","week","month"  default:"day"
    :return:
    """
    # 获取co_co_01        unique_kind: 90 退货  10 普通订单    pmt_status:  0 未付款  1 收款完成
    co_co_01 = spark.sql(
        "select  * from DB2_DB2INST1_CO_CO_01 where (unique_kind = 90 and pmt_status=0) or (unique_kind=10 and pmt_status=1)") \
        .where(col("com_id").isin(["011114305", "011114306", "011114302"]))\
        .withColumn("born_date", f.to_date("born_date", "yyyyMMdd")) \
        .withColumn("today", f.current_date()) \
        .withColumn("qty_sum", col("qty_sum").cast("float")) \
        .withColumn("amt_sum", col("amt_sum").cast("float"))

    lower = scope[0]
    upper = scope[1]
    if lower >= 0 and upper >= lower:
        if filter in ["day","week","month"]:
            if filter=="day":
                co_co_01 = co_co_01.withColumn("day_diff", f.datediff(col("today"), col("born_date"))) \
                    .where((col("day_diff") >= lower) & (col("day_diff") <= upper))

            elif filter=="week":
                co_co_01=co_co_01.withColumn("week_diff", week_diff("today", "born_date"))\
                         .where((col("week_diff") >= lower) & (col("week_diff") <= upper))
            else:
                co_co_01 = co_co_01.withColumn("month_diff",
                                               month_diff_udf(f.year(col("born_date")), f.month(col("born_date")),
                                                              f.year(col("today")), f.month(col("today")))) \
                    .where((col("month_diff") >= lower) & (col("month_diff") <= upper))
        else:
            raise Exception("filter is 'day'/'week'/'month'")
    else:
        raise Exception("lower  must >=0 and upper  must >= lower")

    return co_co_01


def get_co_co_line(spark,scope:list,filter="day"):
    """

        :param spark: SparkSession
        :param scope: [lower,upper]  日期过滤范围
        :param filter: 过滤类别  "day","week","month"  default:"day"
        :return:
        """
    co_co_line = spark.sql(
        "select * from DB2_DB2INST1_CO_CO_LINE") \
        .where(col("com_id").isin(["011114305", "011114306", "011114302"]))\
        .withColumn("born_date", f.to_date("born_date", "yyyyMMdd")) \
        .withColumn("today", f.current_date()) \
        .withColumn("qty_ord", col("qty_ord").cast("float")) \
        .withColumn("price", col("price").cast("float"))

    lower = scope[0]
    upper = scope[1]
    if lower >= 0 and upper >= lower:
        if filter in ["day", "week", "month"]:
            if filter == "day":
                co_co_line = co_co_line.withColumn("day_diff", f.datediff(col("today"), col("born_date"))) \
                    .where((col("day_diff") >= lower) & (col("day_diff") <= upper))

            elif filter == "week":
                co_co_line = co_co_line.withColumn("week_diff", week_diff("today", "born_date")) \
                    .where((col("week_diff") >= lower) & (col("week_diff") <= upper))
            else:
                co_co_line = co_co_line.withColumn("month_diff",
                                               month_diff_udf(f.year(col("born_date")), f.month(col("born_date")),
                                                              f.year(col("today")), f.month(col("today")))) \
                    .where((col("month_diff") >= lower) & (col("month_diff") <= upper))
        else:
            raise Exception("filter is 'day'/'week'/'month'")
    else:
        raise Exception("lower  must >=0 and upper  must >= lower")

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
    #卷烟信息  全量更新
    # ------------------获取plm_item
    plm_item = spark.sql("select * from DB2_DB2INST1_PLM_ITEM where dt=(select max(dt) from DB2_DB2INST1_PLM_ITEM)")
    return plm_item





#--------------外部数据

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
            # .withColumn("cust_id", fill_0_udf(col("cust_id"))) \#一些cust_id前面少个0 在前面补0,在一些指标计算(消费水平)过程中报 miss cust_id。这条语句放到具体计算指标的方法里面执行
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
    pass
