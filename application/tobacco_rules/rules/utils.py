#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/4/8 16:39
import os
import math
import traceback as tb
from datetime import datetime as dt
from pyspark.sql.types import FloatType,IntegerType
from pyspark.sql.functions import udf,date_trunc,datediff,col
from pyspark.sql import functions as f
from pyspark.sql import Column
from pyspark.sql import Window
from ml.cigar_rating import recommendForAllUsers
from rules.config import cities,zkUrl,rent_path,food_path,hotel_path,poi_path,center_path,\
              lng_lat_path,shaoyang_stat_path,yueyang_stat_path,zhuzhou_stat_path, \
              population_path,avg_vfr_path,cons_level_path,cluster_path,brand_table,cigar_rating_path, \
              shaoyang_vfr_path,zhuzhou_vfr_path,yueyang_vfr_path
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



#根据两个点的经纬度，计算这两个点的球面距离
def haversine(lng1:Column,lat1:Column,lng2:Column,lat2:Column):
      radius = 6378137
      #将度数转成弧度
      radLng1 = f.radians(lng1)
      radLat1 = f.radians(lat1)
      radLng2 = f.radians(lng2)
      radLat2 = f.radians(lat2)

      result=f.asin(
            f.sqrt(
                  f.pow(f.sin((radLat1 - radLat2) / 2.0), 2) +
                  f.cos(radLat1) * f.cos(radLat2) * f.pow(f.sin((radLng1 - radLng2) / 2.0), 2))
      )*2.0 * radius
      return result



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


#按照索引 获取数组中的值 索引从1开始
element_at=udf(lambda x,y:x[y-1])




def item_name_etl(df):
    #去掉空格  去掉f或F开头的f或F  去掉*   中文括号用英文括号替换
    # 处理item_name
    df_etl = df\
            .withColumn("item_name", f.trim(col("item_name"))) \
            .withColumn("item_name", f.regexp_replace(col("item_name"), "\*|^f|^F", "")) \
            .withColumn("item_name", f.regexp_replace(col("item_name"), "\（", "\(")) \
            .withColumn("item_name", f.regexp_replace(col("item_name"), "\）", "\)"))
    # 获取brand_name 并处理
    result=df_etl.withColumn("brand_name", element_at(f.split("item_name", "\("), f.lit(1))) \
                    .withColumn("brand_name", f.trim(col("brand_name")))
    return result

def item_name_udf(item_name:Column):
    """

    :param item_name: Column
    :return:
    """
    # 去掉空格  去掉f或F开头的f或F  去掉*   中文括号用英文括号替换
    item_name = f.trim(item_name)
    item_name = f.regexp_replace(item_name, "\*|^f|^F", "")
    item_name = f.regexp_replace(item_name, "\（", "\(")
    item_name = f.regexp_replace(item_name, "\）", "\)")
    brand_name = element_at(f.split(item_name, "\("), f.lit(1))
    brand_name = f.trim(brand_name)
    return brand_name


from sklearn.neighbors import KNeighborsRegressor

def fillWithKNN(df, df_toFill, column):
    """

    :param df: 有值的pandas DataFrame
    :param df_toFill: 需要填充的pandas DataFrame
    :param column: 填充的列名
    :return:
    """

    df[column] = df[column].astype("float")
    X_train = df[['lat', 'lng']]
    y_train = df[[column]]

    X_test = df_toFill[['lat', 'lng']]

    estimator = KNeighborsRegressor()
    estimator.fit(X_train, y_train)
    df_toFill[column] = estimator.predict(X_test)

    return df_toFill



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


def poi_index(spark,cust_lng_lat,coordinate, regex,is_fill=False):
    """
    :param cust_lng_lat:DataFrame,列:city cust_id lng lat lng_l lng_r lat_d lat_u
    :param coordinate:poi 列:cityname lng lat types
    :param regex:  过滤poi的正则
    :param is_fill: 是否填充
    :return DataFrame city cust_id poi_index
    """

    print(f"{str(dt.now())} poi指数  {regex}")
    # coordinate先过滤符合条件的服务 再去join零售户, 在按照city cust_id分组
    count_df = coordinate.where(col("types").rlike(regex)) \
                        .join(cust_lng_lat.drop("lng","lat"),
                              (col("cityname")==col("city")) &
                              (col("lng") >= col("lng_l")) & (col("lng") <= col("lng_r"))
                              & (col("lat") >= col("lat_d")) & (col("lat") <= col("lat_u"))) \
                        .groupBy("city", "cust_id").count()


    if is_fill ==True:
        # 周围有对应店铺数量的零售户
        exist_df = count_df.join(cust_lng_lat, ["city", "cust_id"]) \
            .select("city", "cust_id", "lng", "lat", "count")

        # 周围没有对应店铺的零售户
        not_df = cust_lng_lat.select("city", "cust_id") \
            .exceptAll(count_df.select("city", "cust_id")) \
            .join(cust_lng_lat, ["city", "cust_id"]) \
            .select("city", "cust_id", "lng", "lat")

        if not_df.count()>0:
            # 用knn填充没有店铺数量的零售户
            fill_pd_df = fillWithKNN(exist_df.toPandas(), not_df.toPandas(), "count")
            fill_df = spark.createDataFrame(fill_pd_df)

            all_df = exist_df.unionByName(fill_df)
        else:
            all_df=exist_df



        # 计算阈值threshold =mean(对应数量)+3*std(对应数量)
        threshold = all_df.groupBy("city").agg((f.mean("count") + 3 * f.stddev_pop("count")).alias("threshold"),
                                               f.max("count"))
        # 如果数量大于threshold，就用threshold替换
        replace_df = all_df.join(threshold, "city") \
            .withColumn("count", f.when(col("count") > col("threshold"), col("threshold"))
                        .otherwise(col("count"))
                        )

        result=replace_df
    else:
        result=count_df


    # 全市所有店铺同等计算方式的最大值
    count_max = result.groupBy("city").agg(f.max("count").alias("max"))

    # result
    return result.join(count_max, "city").withColumn("poi_index", col("count") / col("max") * 5)


def fill_dp(spark,cust_lng_lat, dp, colName,coordinate, regex):
    """
        填充价格
    :param cust_lng_lat:零售户经纬度DataFrame,列: city cust_id lng lat lng_l lng_r lat_d lat_u
    :param dp:零售户房租/餐饮/酒店的平均价格DataFrame, 列: city cust_id {colName}
    :param colName:价格字段
    :param coordinate:poi数据DataFrame, 列: lng lat types
    :param regex: 过滤poi的正则
    """
    # 列:city,cust_id,poi_index
    poi_index_df = poi_index(spark, cust_lng_lat,coordinate, regex)
    # 得到包含所有零售户的DataFrame
    df0 = cust_lng_lat.join(poi_index_df, ["city", "cust_id"], "left") \
                        .join(dp, ["city", "cust_id"], "left") \
                        .select("city", "cust_id", colName, "poi_index", "lng", "lat")

    summary = df0.select(colName, "poi_index").summary().cache()

    # 全市平均价格的min,max及各分位数
    price_min = summary.where(col("summary") == "min").collect()[0][colName]
    price_25 = summary.where(col("summary") == "25%").collect()[0][colName]
    price_50 = summary.where(col("summary") == "50%").collect()[0][colName]
    price_75 = summary.where(col("summary") == "75%").collect()[0][colName]
    price_max = summary.where(col("summary") == "max").collect()[0][colName]

    # 全市对应poi指数的min,max及各分位数
    poi_25 = summary.where(col("summary") == "25%").collect()[0]["poi_index"]
    poi_50 = summary.where(col("summary") == "50%").collect()[0]["poi_index"]
    poi_75 = summary.where(col("summary") == "75%").collect()[0]["poi_index"]
    poi_max = summary.where(col("summary") == "max").collect()[0]["poi_index"]

    # 零售户周边既没有poi也没有价格的零售户，先用0填充
    df1 = df0.withColumn(colName, f.when(col(colName).isNull() & col("poi_index").isNull(), 0)
                         .otherwise(col(colName))
                         )

    # 零售户周边有poi没有价格的，使用分箱填充价格
    df2 = df1.withColumn(colName,
                         f.when((col(colName).isNull()) & (col("poi_index") < poi_25), price_min)
                         .when((col(colName).isNull()) & (col("poi_index") >= poi_25) & (col("poi_index") < poi_50),
                               price_25)
                         .when((col(colName).isNull()) & (col("poi_index") >= poi_50) & (col("poi_index") < poi_75),
                               price_50)
                         .when((col(colName).isNull()) & (col("poi_index") >= poi_75) & (col("poi_index") < poi_max),
                               price_75)
                         .otherwise(price_max)
                         )

    # -----零售户周边价格为0的零售户，使用KNN填充
    # 价格不为0
    exist_df = df2.where(col(colName) > 0).drop("poi_index")
    # 价格为0
    not_df = df2.where(col(colName) == 0).drop("poi_index")


    if not_df.count()>0:
        # knn填充
        fill_pd_df = fillWithKNN(exist_df.toPandas(), not_df.toPandas(), colName)
        # 合并
        all_df = spark.createDataFrame(fill_pd_df).unionByName(exist_df)
    else:
        all_df=exist_df

    #-----截断
    #计算每个城市的 threshold
    threshold = all_df.groupBy("city")\
                     .agg((f.mean(col(colName)) + 3 * f.stddev_pop(col(colName))).alias("threshold"))
    truncate_df = all_df.join(threshold, "city") \
                        .withColumn(colName,
                                    f.when(col(colName) > col("threshold"), col("threshold"))
                                    .otherwise(col(colName))
                                    )
    #计算价格+1 的对数
    log_df = truncate_df.withColumn("log", f.log(col(colName) + 1))

    log_max = log_df.groupBy("city").agg(f.max("log").alias("log_max"))

    result=log_df.join(log_max, "city") \
                .withColumn(colName+"_index", col("log") / col("log_max") * 5)
    #'city','cust_id','food_avg','poi_index','lng','lat','threshold','log','log_max','{colName}_index'
    return result






def get_consume_level(spark):
    """
       零售户周边(1km)消费水平
       由于外部数据的不足，只有部分零售户有消费水平
       generate_data#generate_all_cust_cons生成所有的零售户的消费水平
    :param spark: SparkSession
    :return:
    """
    # 租金 餐饮 酒店信息

    dp = {"rent": rent_path,
           "food":food_path,
           "hotel": hotel_path}

    try:
        cust_lng_lat = get_cust_lng_lat(spark).select("city", "cust_id", "lng", "lat") \
            .withColumn("scope", f.lit(1)) \
            .withColumn("lng_l", lng_l(col("lng"), col("lat"), col("scope"))) \
            .withColumn("lng_r", lng_r(col("lng"), col("lat"), col("scope"))) \
            .withColumn("lat_d", lat_d(col("lat"), col("scope"))) \
            .withColumn("lat_u", lat_u(col("lat"), col("scope"))) \
            .drop("lng", "lat")

        # -------------租金
        print(f"{str(dt.now())}  rent")
        # rent
        rent = spark.read.csv(header=True, path=dp["rent"]) \
            .withColumn("lng", col("longitude").cast("float")) \
            .withColumn("lat", col("latitude").cast("float")) \
            .select("lng", "lat", "price_1square/(元/平米)") \
            .dropna(subset=["lng", "lat", "price_1square/(元/平米)"])

        # 零售户一公里的每平米租金
        city_rent = cust_lng_lat.join(rent, (col("lng") >= col("lng_l")) & (col("lng") <= col("lng_r")) & (
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
        # food
        food = spark.read.csv(header=True, path=dp["food"]) \
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

        # hotel
        hotel = spark.read.csv(header=True, path=dp["hotel"]) \
            .withColumn("lng", col("lng").cast("float")) \
            .withColumn("lat", col("lat").cast("float")) \
            .select("lng", "lat", "price") \
            .dropna(subset=["lng", "lat", "price"])

        city_hotel = hotel.join(cust_lng_lat, (col("lng") >= col("lng_l")) & (col("lng") <= col("lng_r")) & (
                col("lat") >= col("lat_d")) & (col("lat") <= col("lat_u")))
        # 每个零售户一公里范围内的酒店价格的均值
        city_hotel_avg = city_hotel.groupBy("city", "cust_id").agg(f.mean("price").alias("hotel_avg"))

        # 各市25%分位值
        city_hotel_avg.registerTempTable("city_hotel_avg")
        city_hotel_avg = spark.sql(
            "select city,percentile_approx(hotel_avg,0.25) as hotel_25 from city_hotel_avg group by city") \
            .join(city_hotel_avg, "city")

        print(f"{str(dt.now())}   consume level")
        consume_level_df = city_rent_avg.join(city_food_avg, ["city", "cust_id"], "outer") \
                                        .join(city_hotel_avg, ["city", "cust_id"], "outer") \
                                        .fillna(0, ["rent_25", "food_25", "hotel_25"]) \
                                        .withColumn("consume_level",
                                                    consume_level_udf(col("rent_avg"), col("food_avg"), col("hotel_avg"), col("rent_25"),
                                                                      col("food_25"), col("hotel_25")))

        return consume_level_df
    except Exception:
        tb.print_exc()


def around_except_grade(around_cust, value_df,cust_cluster,cols:dict, grade=[3, 4, 5]):
    """
          零售户指标与周围零售户相比
    :param around_cust: 列:cust_id1,cust_id0  零售户cust_id1周边包含cust_id0这些零售户
    :param value_df:  列:cust_id,cols["value"]  cols["value"]:计算均值、标准差的值  如 条均价/订单额
    :param cust_cluster: 列:cust_id,cluster_index  零售户聚类结果
    :param cols: {"value":value,}
    :param grade: [3,4,5] 按照 3/4/5倍标准差分档
    :return:
    """

    value = cols["value"]

    abnormal = "warning_level_code"
    plus_one_grade="avg_orders_plus3"
    minus_one_grade="avg_orders_minu3"
    plus_two_grade="avg_orders_plus4"
    minus_two_grade="avg_orders_minu4"
    plus_three_grade="avg_orders_plus5"
    minus_three_grade="avg_orders_minu5"

    # 1.获取cust_id1的类别
    # 2.获取cust_id0的类别
    around_cust = around_cust.join(cust_cluster, col("cust_id") == col("cust_id1")) \
                            .drop("cust_id") \
                            .withColumnRenamed("cluster_index", "cluster_index1") \
                            .join(cust_cluster, col("cust_id") == col("cust_id0")) \
                            .withColumnRenamed("cluster_index", "cluster_index0") \
                            .where(col("cluster_index0") == col("cluster_index1")) \
                            .select("cust_id1", "cust_id0")

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
        均值 + 4标准差 < 条均价 < 均值 + 5标准差   过高  21
        均值 - 5标准差 < 条均价 < 均值 - 4标准差   过低  20
    预警A
        均值 + 5标准差 < 条均价    过高  31
        条均价 < 均值 - 5标准差   过低  30
    """
    except_cust = mean_std.join(value_df, col("cust_id1") == col("cust_id")) \
        .withColumn(plus_one_grade, col("mean") + col("stddev") * grade[0]) \
        .withColumn(minus_one_grade, col("mean") - col("stddev") * grade[0]) \
        .withColumn(plus_two_grade, col("mean") + col("stddev") * grade[1]) \
        .withColumn(minus_two_grade, col("mean") - col("stddev") * grade[1]) \
        .withColumn(plus_three_grade, col("mean") + col("stddev") * grade[2]) \
        .withColumn(minus_three_grade, col("mean") - col("stddev") * grade[2]) \
        .withColumn(abnormal,
                    f.when((col(value) > col(plus_one_grade)) & (col(value) < col(plus_two_grade)), "C1")
                    .when((col(value) > col(minus_two_grade)) & (col(value) < col(minus_one_grade)), "C0")
                    .when((col(value) > col(plus_two_grade)) & (col(value) < col(plus_three_grade)), "B1")
                    .when((col(value) > col(minus_three_grade)) & (col(value) < col(minus_two_grade)), "B0")
                    .when((col(value) > col(plus_three_grade)), "A1")
                    .when((col(value) < col(minus_three_grade)), "A0")
        ).dropna(subset=[abnormal])

    return except_cust


def except_grade(df, cols: dict, groupBy: list, grade: list):
    """
        零售户指标与 {groupBy} 零售户相比
    :param df: 包含的columns:cust_id com_id cust_seg value
    :param cols:   列:cust_id,cols["value"]  cols["value"]:计算均值、标准差的值  如 条均价/订单额
    :param groupBy: list   [city]/[city,cluster_index]   按照city或 city和聚类结果 分组
    :param grade: [3,4,5] 按照 3/4/5倍标准差分档
    """

    value = cols["value"]

    abnormal = "warning_level_code"
    plus_one_grade="avg_orders_plus3"
    minus_one_grade="avg_orders_minu3"
    plus_two_grade="avg_orders_plus4"
    minus_two_grade="avg_orders_minu4"
    plus_three_grade="avg_orders_plus5"
    minus_three_grade="avg_orders_minu5"


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
                    f.when((col(value) > col(plus_one_grade)) & (col(value) < col(plus_two_grade)), "C1")
                    .when((col(value) > col(minus_two_grade)) & (col(value) < col(minus_one_grade)), "C0")
                    .when((col(value) > col(plus_two_grade)) & (col(value) < col(plus_three_grade)), "B1")
                    .when((col(value) > col(minus_three_grade)) & (col(value) < col(minus_two_grade)), "B0")
                    .when((col(value) > col(plus_three_grade)), "A1")
                    .when((col(value) < col(minus_three_grade)), "A0")
                    ).dropna(subset=[abnormal])
    return result



def get_around_cust(spark,around:int):
    """
     获取零售户cust_id1 包含cust_id0这些零售户
    :param around: 周围 {around} km
    """
    #零售户经纬度
    cust_lng_lat=get_cust_lng_lat(spark).withColumnRenamed("cust_id", "cust_id0") \
                                        .withColumnRenamed("city", "city0") \
                                        .withColumnRenamed("lng", "lng0") \
                                        .withColumnRenamed("lat", "lat0") \
                                        .select("city0","cust_id0","lng0","lat0")
    #每个零售户  {around}公里的经度范围和纬度范围
    cust_lng_lat1 = cust_lng_lat.withColumn("scope",f.lit(around))\
                                .withColumn("lng_l", lng_l(col("lng0"), col("lat0"),col("scope"))) \
                                .withColumn("lng_r", lng_r(col("lng0"), col("lat0"),col("scope"))) \
                                .withColumn("lat_d", lat_d(col("lat0"),col("scope"))) \
                                .withColumn("lat_u", lat_u(col("lat0"),col("scope"))) \
                                .withColumnRenamed("cust_id0", "cust_id1") \
                                .withColumnRenamed("city0", "city1") \
                                .withColumnRenamed("lng0", "lng1") \
                                .withColumnRenamed("lat0", "lat1") \
                                .select("city1","cust_id1","lng1","lat1", "lng_l", "lng_r", "lat_d", "lat_u")
    #每个零售户cust_id1 周边有cust_id0这些零售户
    around_cust=cust_lng_lat.join(cust_lng_lat1,(col("city1")==col("city0"))&(col("lng0")>=col("lng_l")) & (col("lng0")<=col("lng_r")) & (col("lat0")>=col("lat_d")) & (col("lat0")<=col("lat_u")))\
                            .select("city1","cust_id1","lng1","lat1","city0","cust_id0","lng0","lat0")
    return around_cust


def get_rating(spark,group):
    """

    :param spark:
    :param group:取brand_name或item_id，item_id:计算零售户对品规的评分
                brand_name:计算零售户对品牌的评分
    :return: cust_id,brand_name/item_id,rating
    """
    # 卷烟id 卷烟名称
    plm_item = get_plm_item(spark) \
                      .where((col("is_mrb") == "1") & (col("item_kind") != "4")) \
                      .select("item_id", "item_name")

    #status:01,02 and  cust_seg!=ZZ
    co_cust=get_valid_co_cust(spark)\
                        .where(col("cust_seg")!='ZZ')

    # 开始计算评分
    # 1.获取近30天数据 并删除缺失值
    co_co_line = get_co_co_line(spark, scope=[0, 30]) \
        .join(co_cust, "cust_id") \
        .join(plm_item,"item_id")\
        .withColumn("brand_name", item_name_udf(col("item_name"))) \
        .select("cust_id", group, "qty_ord", "qty_rsn") \
        .na.drop()


    # 2.获取每个零售户对每款卷烟的订足率 并删除null值
    item_rating = co_co_line.groupBy("cust_id", group) \
        .agg(f.sum("qty_ord").alias("qty_ord"), f.sum("qty_rsn").alias("qty_rsn")) \
        .withColumn("rating", col("qty_ord") / col("qty_rsn")) \
        .na.drop().select("cust_id", group, "rating")
    # 3.每个零售户对每款烟的一个评分
    result = recommendForAllUsers(spark, item_rating, "cust_id", group, "rating")

    return result












#---------------------------------数据集--------------------------------

def get_co_cust(spark):
    # -----------------co_cust 零售客户信息表   全量更新  选取dt最新的数据
    co_cust=spark.sql("select * from DB2_DB2INST1_CO_CUST where dt=(select max(dt) from DB2_DB2INST1_CO_CUST) and status !=04") \
                      .where(col("com_id").isin(cities))
    return co_cust
def get_valid_co_cust(spark):
    #获取有效的零售户
    co_cust=spark.sql("select * from DB2_DB2INST1_CO_CUST where dt=(select max(dt) from DB2_DB2INST1_CO_CUST)")\
                 .where((col("status").rlike("(01)|(02)")) & (col("com_id").isin(cities)))
    return co_cust


def get_crm_cust(spark):
    # ----------------crm_cust 零售客户信息表 为co_cust表的补充  全量更新  选取dt最新的数据
    crm_cust=spark.sql("select * from DB2_DB2INST1_CRM_CUST where dt=(select max(dt) from DB2_DB2INST1_CRM_CUST)") \
                    .where(col("com_id").isin(cities))\
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
        .where(col("com_id").isin(cities)) \
        .where(col("born_date").rlike("\d{8}")) \
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
        .where(col("com_id").isin(cities))\
        .where(col("born_date").rlike("\d{8}"))\
        .withColumn("born_date", f.to_date("born_date", "yyyyMMdd")) \
        .withColumn("today", f.current_date()) \
        .withColumn("qty_ord", col("qty_ord").cast("float")) \
        .withColumn("price", col("price").cast("float"))\
        .withColumn("qty_rsn",col("qty_rsn").cast("float"))

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









#----------------------------外部数据

def get_coordinate(spark):
    #poi数据  餐厅、交通、商城、娱乐场馆等经纬度
    coordinate = spark.read.csv(header=True, path=poi_path) \
        .where(col("cityname").isin(["株洲市","邵阳市","岳阳市"]))\
        .withColumn("lng", col("longitude").cast("float")) \
        .withColumn("lat", col("latitude").cast("float"))
    return coordinate

def get_area(spark):
    """
    获取city sale_center_id county的对应关系
    :param spark:
    :return:
    """
    area_code = spark.read.csv(path=center_path, header=True) \
                .where(col("com_id").isin(cities))\
                .withColumnRenamed("城市", "city") \
                .withColumnRenamed("区", "county")
    return area_code


def get_cust_lng_lat(spark):
    """
    获取零售户经纬度 column:city cust_id lng lat
    :param spark:
    :return:
    """

    lng_lat = spark.read.csv(header=True, path=lng_lat_path) \
                        .withColumn("lng", col("lng").cast("float")) \
                        .withColumn("lat", col("lat").cast("float")) \
                        .dropna(how="any", subset=["lng", "lat"])
    return lng_lat


def get_city_info(spark):
    """
    获取统计数据  gdp 第一、二、三产业总量等
    :param spark:
    :return:
    """
    #因为一些字段数据不统一，分开读
    cities = {"邵阳市": shaoyang_stat_path,
              "岳阳市": yueyang_stat_path,
              "株洲市": zhuzhou_stat_path}
    dfs = []
    for city in cities.keys():
        path = cities[city]
        city_gdp = spark.read.csv(path, header=True) \
            .withColumn("city", f.lit(city))
        if city == "邵阳市":
            city_gdp = city_gdp.withColumn("mtime", f.to_date("mtime", "yy-MMM"))
        elif city == "岳阳市":
            city_gdp = city_gdp.withColumn("mtime", f.to_date("mtime", "MMM-yy"))
        else:
            city_gdp=city_gdp.withColumn("mtime",f.to_date("mtime","yyyy/MM/dd"))

        dfs.append(city_gdp)

    df = dfs[0]
    for i in range(1, len(dfs)):
        df = df.unionByName(dfs[i])

    return df

def get_city_ppl(spark):
    """
    获取人口数据
    :param spark:
    :return:
    """
    population=spark.read.csv(population_path,header=True) \
                       .withColumn("区县", f.regexp_replace(col("区县"), "城步县", "城步苗族自治县"))
    cols = population.columns
    cols.remove("区县")
    for c in cols:
        population = population.withColumn(c, f.col(c).cast("float"))
    return population

def get_vfr(spark):
    """
     读取人流原始数据
    :param spark:
    """
    cities = {"邵阳市": shaoyang_vfr_path, "岳阳市": yueyang_vfr_path, "株洲市": zhuzhou_vfr_path}
    dfs = []
    for city in cities:
        path = cities[city]

        df = spark.read.csv(path, header=True).withColumn("city", f.lit(city))
        dfs.append(df)

    df = dfs[0]
    for i in range(1, len(dfs)):
        df = df.unionByName(dfs[i])
    return df


def get_rent(spark):
    rent = spark.read.csv(header=True, path=rent_path) \
        .withColumn("city",f.trim(col("市")))\
        .withColumn("lng", col("longitude").cast("float")) \
        .withColumn("lat", col("latitude").cast("float")) \
        .withColumn("price",col("price_1square/(元/平米)").cast("float"))\
        .select("city","lng", "lat", "") \
        .dropna(subset=["lng", "lat", "price"])
    return rent
def get_food(spark):
    food = spark.read.csv(header=True, path=food_path) \
        .withColumn("city", f.trim(col("city"))) \
        .withColumn("city", f.when(col("city") == "shaoyang", "邵阳市")
                             .when(col("city") == "zhuzhou", "株洲市")
                             .when(col("city") == "yueyang", "岳阳市")
                    ) \
        .withColumn("lng", col("lng").cast("float")) \
        .withColumn("lat", col("lat").cast("float")) \
        .withColumn("price", col("mean_prices").cast("float")) \
        .select("city","lng", "lat", "mean_prices") \
        .dropna(subset=["lng", "lat", "price"])
    return food
def get_hotel(spark):
    hotel = spark.read.csv(header=True, path=hotel_path) \
        .withColumn("city",f.trim(col("city")))\
        .withColumn("city",f.when(col("city")=="shaoyang","邵阳市")
                             .when(col("city")=="zhuzhou","株洲市")
                             .when(col("city")=="yueyang","岳阳市")
                    )\
        .withColumn("lng", col("lng").cast("float")) \
        .withColumn("lat", col("lat").cast("float")) \
        .withColumn("price",col("price").cast("float"))\
        .select("city","lng", "lat", "price") \
        .dropna(subset=["lng", "lat", "price"])
    return hotel


def get_phoenix_table(spark,tableName):
    ph_df=spark.read.format("org.apache.phoenix.spark")\
            .option("table",tableName)\
            .option("zkUrl",zkUrl)\
            .load()
    columns=ph_df.columns
    for column in columns:
        ph_df=ph_df.withColumnRenamed(column,column.lower())
    return ph_df





#---------------获取生成的外部数据
def get_near_cust(spark):
    """
     已移除根据最近30个零售户来计算
     generate_data.py#generate_near_cust(spark)生成的
     YJFL001指标
     获取距离零售户最近的30个零售户
    :param spark:
    :return:
    """
    dir = "/user/entrobus/tobacco_data/cust_cross_join/"
    paths = ["yueyang", "shaoyang", "zhuzhou"]
    dfs = []
    for i in range(len(paths)):
        path = paths[i]
        df = spark.read.csv(dir + path, header=True).where(col("rank") <= 30)
        dfs.append(df)

    df = dfs[0]
    for i in range(1, len(dfs)):
        df = df.unionByName(dfs[i])
    return df

def get_around_vfr(spark):
    """
    零售户周边人流数=零售户半径500m范围内平均人流
    平均人流=近30天所有记录中距离中心点最近的100个观测记录的均值（距离不超过500m，若不足100个则有多少算多少）
    """
    paths = ["岳阳市", "邵阳市", "株洲市"]
    dfs=[]
    for i in range(len(paths)):
        path=paths[i]
        whole_path=os.path.join(avg_vfr_path, path)
        avg_vfr = spark.read.csv(whole_path,header=True)\
                      .withColumn("avg_vfr",col("avg_vfr").cast("float"))
        dfs.append(avg_vfr)


    df=dfs[0]
    for i in range(1,len(dfs)):
        df=df.unionByName(dfs[i])

    return df


def get_all_consume_level(spark):
    #获取所有的零售户的消费水平
    consume_level_df=spark.read.csv(path=cons_level_path,header=True)\
                                 .withColumn("consume_level",col("consume_level").cast("float"))
    return consume_level_df

def get_cigar_rating(spark):
    """
    获取每个零售户对每个品规的评分
    :param spark:
    :return:
    """
    cigar_rating=spark.read.parquet(cigar_rating_path)
    return cigar_rating


def get_cust_cluster(spark):
    """
      获取零售户聚类结果
    :param spark:
    :return:
    """
    # cust_id cluster_index
    cities = ["邵阳市", "岳阳市", "株洲市"]

    dfs = []
    for city in cities:
        path = os.path.join(cluster_path, city)
        df = spark.read.csv(path, header=True) \
            .withColumn("city", f.lit(city))
        dfs.append(df)

    cluster_result = dfs[0]
    for i in range(1, len(dfs)):
        cluster_result = cluster_result.unionByName(dfs[i])
    return cluster_result.drop("city")
if __name__=="__main__":
    pass
