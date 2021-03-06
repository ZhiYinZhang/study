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
from rules.write_hbase import write_hbase1
from rules.utils import *
from rules.config import cities,retail_table

"""
零售户画像
"""

spark = SparkSession.builder\
                    .enableHiveSupport()\
                    .appName("retail")\
                    .getOrCreate()
sc=spark.sparkContext
sc.setLogLevel("WARN")

spark.sql("use aistrong")

hbase={"table":retail_table,"families":["0"],"row":"cust_id"}
# hbase={"table":"test1","families":["0"],"row":"cust_id"}


def get_cust_info():
    #零售户信息表co_cust
    co_cust_cols=["cust_id","cust_name","cust_seg","status","pay_type","license_code","manager","identity_card_id",
                  "order_tel","inv_type","order_way","periods","busi_addr","work_port","base_type","sale_scope",
                  "scope","com_chara","is_tor_tax","is_sale_large","is_rail_cust","area_type","is_sefl_cust","is_func_cust",
                  "sale_center_id","sale_dept_id","slsman_id","slsman_mobile"]
    # 需要修改的列名   与hbase的列名对应
    co_cust_renamed = {"is_tor_tax": "tor_tax", "is_sale_large": "sale_large",
                       "is_rail_cust": "rail_cust", "is_sefl_cust": "sefl_cust",
                       "is_func_cust": "func_cust", "cust_seg": "grade"
                       }
    #零售户信息表crm_cust
    crm_cust_cols=["cust_id","longitude","latitude","is_multiple_shop","org_model","is_night_shop",
                   "busi_time_type","consumer_group","consumer_attr","busi_type","compliance_grade",
                   "rim_type","info_terminal"]
    crm_cust_renamed = {"is_multiple_shop": "multiple_shop", "is_night_shop": "night_shop"}


    #送货地址
    ldm_cust = spark.sql(
        "select cust_id,dist_addr from DB2_DB2INST1_LDM_CUST where dt=(select max(dt) from DB2_DB2INST1_LDM_CUST)")
    #是否属于配送点
    ldm_cust_dist = spark.sql(
        "select cust_id,is_in_point from DB2_DB2INST1_LDM_CUST_DIST where dt=(select max(dt) from DB2_DB2INST1_LDM_CUST_DIST)")



    print(f"{str(dt.now())} co_cust/crm_cust零售户信息")
    try:
        co_cust = spark.sql("select * from DB2_DB2INST1_CO_CUST where dt=(select max(dt) from DB2_DB2INST1_CO_CUST)") \
                                 .where(col("com_id").isin(cities)).select(co_cust_cols)
        #修改列名
        for key in co_cust_renamed.keys():
            co_cust = co_cust.withColumnRenamed(key, co_cust_renamed[key])


        crm_cust=get_crm_cust(spark).select(crm_cust_cols)
        #修改列名
        for key in crm_cust_renamed.keys():
            crm_cust = crm_cust.withColumnRenamed(key, crm_cust_renamed[key])


        cust_info=co_cust.join(crm_cust, "cust_id","left")\
                          .join(ldm_cust,"cust_id","left")\
                          .join(ldm_cust_dist,"cust_id","left")
        cols=cust_info.columns
        cust_info.foreachPartition(lambda x: write_hbase1(x, cols, hbase))
    except Exception:
        tb.print_exc()

# get_cust_info()



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

# get_acc()



def get_city_county():
    #-----市 区
    print(f"{str(dt.now())} city county abcode")
    try:
        co_cust=get_co_cust(spark).select("cust_id","com_id","sale_center_id")

        area_code=get_area(spark).groupBy("com_id","city","sale_center_id")\
                         .agg(f.collect_list("county").alias("county"))

        co_cust.join(area_code,["com_id","sale_center_id"])\
               .withColumnRenamed("城市","city")\
               .withColumnRenamed("区","county")\
               .foreachPartition(lambda x: write_hbase1(x, ["city","county"],hbase))
    except Exception:
        tb.print_exc()

# get_city_county()





def get_lng_lat():
    #-----网上爬取的经纬度
    print(f"{str(dt.now())}   经纬度")
    try:
        co_cust=get_co_cust(spark).select("cust_id")

        #零售户的经纬度
        cust_lng_lat=get_cust_lng_lat(spark).select("cust_id","lng","lat")

        lng_lat=cust_lng_lat.withColumnRenamed("lng","longitude")\
                            .withColumnRenamed("lat","latitude")\
                            .join(co_cust,"cust_id")

        lng_lat.foreachPartition(lambda x: write_hbase1(x, ["longitude", "latitude"], hbase))
    except Exception:
       tb.print_exc()

# get_lng_lat()






def get_30_grade_change():
    co_cust=get_co_cust(spark).select("cust_id")
    crm_cust_log=get_crm_cust_log(spark).where(col("day_diff") <= 30)\
                      .join(co_cust,"cust_id")
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

# get_30_grade_change()





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
        result.foreachPartition(lambda x: write_hbase1(x, ["license_not_match"],hbase))
    except Exception:
        tb.print_exc()

# card_passId()





# 是否隐形连锁户??????????





#零售户周边餐饮消费指数（餐厅，咖啡厅，茶艺馆，甜点店等的数量）
#零售户周边交通便捷指数（地铁站，公交车站，机场，港口码头数量）
#零售户周边购物消费指数（购物中心，普通商场，免税品店，市场，特色商业街，步行街，农贸市场数量）
#零售户周边娱乐休闲指数（运动场馆，娱乐场所，休闲场所数量）

"""
计算方式：
part1：对零售户所在位置一定范围内没有对应店铺数量的零售户用knn进行填充
part2：
    step1：threshold = mean(零售户所在位置一定范围内对应店铺数量) + 5*std(零售户所在位置一定范围内对应店铺数量)
    step2：零售户所在位置一定范围内对应店铺数量大于threshold的店铺数量等于threshold
    step3：利用更新后的对应店铺数量计算：
    零售户所在位置一定范围内（1000m）对应店铺数量/全市所有店铺同等计算方式的最大值*5
"""
def get_cust_index():
    types = {"catering_cons_count": "(.*餐厅.*)|(.*咖啡厅.*)|(.*茶艺馆.*)|(.*甜品.*)",
             "convenient_trans_count": "(.*地铁站.*)|(.*公交.*)|(.*机场.*)|(.*港口码头.*)",
             "shopping_cons_count": "(.*购物中心.*)|(.*普通商场.*)|(.*免税品店.*)|(.*市场.*)|(.*特色商业街.*)|(.*步行街.*)|(.*农贸市场.*)",
             "entertainment_count": "(.*运动场馆.*)|(.*娱乐场所.*)|(.*休闲场所.*)"}
    try:
        # 餐厅、交通、商城、娱乐场馆等经纬度
        coordinate = get_coordinate(spark).select("cityname","lng", "lat", "types")

        coordinate.cache()

        # 零售户经纬度
        co_cust = get_co_cust(spark).select("cust_id")
        cust_lng_lat = get_cust_lng_lat(spark).select("cust_id", "city", "lng", "lat") \
            .join(co_cust, "cust_id")

        # 每个零售户  一公里的经度范围和纬度范围
        cust_lng_lat0 = cust_lng_lat.withColumn("scope", f.lit(1)) \
            .withColumn("lng_l", lng_l(col("lng"), col("lat"), col("scope"))) \
            .withColumn("lng_r", lng_r(col("lng"), col("lat"), col("scope"))) \
            .withColumn("lat_d", lat_d(col("lat"), col("scope"))) \
            .withColumn("lat_u", lat_u(col("lat"), col("scope")))

        cust_lng_lat0.cache()

        for y in range(len(types)):
            try:
                colName = list(types.keys())[y]
                regex = types[colName]

                print(f"{str(dt.now())} {colName}指数")

                result = poi_index(spark, cust_lng_lat0,coordinate,regex, is_fill=True) \
                    .withColumnRenamed("poi_index", colName)

                result.foreachPartition(lambda x: write_hbase1(x, [colName], hbase))
            except Exception:
                tb.print_exc()

        coordinate.unpersist()
        cust_lng_lat0.unpersist()
    except Exception:
        tb.print_exc()


# get_cust_index()





#-----零售户周边消费水平指数
"""
零售户所在位置一定范围内（1000m）对应消费水平/全市所有店铺同等计算方式的最大值*5，
对应消费水平= 所在位置一定范围内（1000m）的（每平方米租金价格均值+餐饮价格均值+酒店价格均值）     
注：1）如果三项同时缺失，按缺失值展示 
2）如有其中一项缺失，按该项所有值从小到大排序，取25%分位处值填充
"""
def get_consume_level_index():
    """
       零售户周边(1km)消费水平
       由于外部数据的不足，只有部分零售户有消费水平
       generate_data#generate_all_cust_cons生成所有的零售户的消费水平
    :param spark: SparkSession
    :return:
    """
    # 租金 餐饮 酒店信息

    dp = {"rent": rent_path,
          "food": food_path,
          "hotel": hotel_path}

    try:
        # poi数据
        coordinate = get_coordinate(spark).select("cityname","lng", "lat", "types")

        coordinate.cache()

        #零售户经纬度
        co_cust = get_valid_co_cust(spark).select("cust_id")
        cust_lng_lat = get_cust_lng_lat(spark) \
            .join(co_cust, "cust_id") \
            .select("city", "cust_id", "lng", "lat")

        #零售户周边
        cust_around = cust_lng_lat.withColumn("scope", f.lit(1)) \
            .withColumn("lng_l", lng_l(col("lng"), col("lat"), col("scope"))) \
            .withColumn("lng_r", lng_r(col("lng"), col("lat"), col("scope"))) \
            .withColumn("lat_d", lat_d(col("lat"), col("scope"))) \
            .withColumn("lat_u", lat_u(col("lat"), col("scope")))

        cust_around.cache()



        # -------------租金
        print(f"{str(dt.now())}  rent")
        # rent
        rent = spark.read.csv(header=True, path=dp["rent"]) \
            .withColumn("lng", col("longitude").cast("float")) \
            .withColumn("lat", col("latitude").cast("float")) \
            .select("lng", "lat", "price_1square/(元/平米)") \
            .dropna(subset=["lng", "lat", "price_1square/(元/平米)"])

        # 零售户一公里的每平米租金
        city_rent = cust_around.drop("lng", "lat").join(rent,
                                                        (col("lng") >= col("lng_l")) & (col("lng") <= col("lng_r")) &
                                                        (col("lat") >= col("lat_d")) & (col("lat") <= col("lat_u")))
        # 每个零售户一公里范围内的每平米租金的均值
        city_rent_avg = city_rent.groupBy("city", "cust_id").agg(f.mean("price_1square/(元/平米)").alias("rent_avg"))


        # 需要的poi指数
        regex = "(.*住宅.*)|(.*别墅.*)|(.*社区中心.*)|(.*便民商店.*)"
        all_rent_avg = fill_dp(spark, cust_around, city_rent_avg, "rent_avg", coordinate, regex) \
            .select("city", "cust_id", "rent_avg_index")



        # --------------餐饮
        print(f"{str(dt.now())}  food")
        # food
        food = spark.read.csv(header=True, path=dp["food"]) \
            .withColumn("lng", col("lng").cast("float")) \
            .withColumn("lat", col("lat").cast("float")) \
            .withColumn("mean_prices", col("mean_prices").cast("float")) \
            .select("lng", "lat", "mean_prices") \
            .dropna(subset=["lng", "lat", "mean_prices"])

        city_food = food.join(cust_around.drop("lng", "lat"),
                              (col("lng") >= col("lng_l")) & (col("lng") <= col("lng_r")) & (
                                      col("lat") >= col("lat_d")) & (col("lat") <= col("lat_u")))
        # 每个零售户一公里范围内的餐饮价格的均值
        city_food_avg = city_food.groupBy("city", "cust_id").agg(f.mean("mean_prices").alias("food_avg"))


        # 需要的poi指数
        regex = "(.*餐厅.*)|(.*咖啡厅.*)|(.*茶艺馆.*)|(.*甜品.*)"
        all_food_avg = fill_dp(spark, cust_around, city_food_avg, "food_avg", coordinate, regex) \
            .select("city", "cust_id", "food_avg_index")




        # --------------酒店
        print(f"{str(dt.now())}  hotel")

        # hotel
        hotel = spark.read.csv(header=True, path=dp["hotel"]) \
            .withColumn("lng", col("lng").cast("float")) \
            .withColumn("lat", col("lat").cast("float")) \
            .select("lng", "lat", "price") \
            .dropna(subset=["lng", "lat", "price"])

        city_hotel = hotel.join(cust_around.drop("lng", "lat"),
                                (col("lng") >= col("lng_l")) & (col("lng") <= col("lng_r")) & (
                                        col("lat") >= col("lat_d")) & (col("lat") <= col("lat_u")))
        # 每个零售户一公里范围内的酒店价格的均值
        city_hotel_avg = city_hotel.groupBy("city", "cust_id").agg(f.mean("price").alias("hotel_avg"))

        # 需要的poi指数
        regex = "(.*宾馆.*)|(.*酒店.*)|(.*招待所.*)"
        all_hotel_avg = fill_dp(spark, cust_around, city_hotel_avg, "hotel_avg", coordinate, regex) \
            .select("city", "cust_id", "hotel_avg_index")

        result = all_rent_avg.join(all_food_avg, ["city", "cust_id"]) \
            .join(all_hotel_avg, ["city", "cust_id"])


        colName="catering_cons_avg"
        result.withColumn(colName,(col("rent_avg_index") + col("food_avg_index") + col("hotel_avg_index")) / 3) \
               .foreachPartition(lambda x: write_hbase1(x, [colName], hbase))


        coordinate.unpersist()
        cust_around.unpersist()
    except Exception:
        tb.print_exc()


# get_consume_level_index()




#-----零售户周边人流指数
"""
visitor flow rate
零售户周边人流指数=零售户周边人流/max(零售户周边人流)*5
零售户周边人流数=零售户半径500m范围内平均人流 
平均人流=近30天所有记录中距离中心点最近的100个观测记录的均值（距离不超过500m，若不足100个则有多少算多少）
"""
def get_vfr_index():
    try:
        print(f"{str(dt.now())} 零售户周边人流指数")
        # 有人流数据的零售户
        vfr = get_around_vfr(spark)

        vfr.cache()

        # 零售户
        co_cust = get_co_cust(spark).select("cust_id")
        # 有经纬度的零售户
        cust_lng_lat = get_cust_lng_lat(spark) \
            .select("city", "cust_id", "lng", "lat") \
            .join(co_cust, "cust_id")

        cust_lng_lat.cache()

        # 周边没有人流的零售户
        not_df = cust_lng_lat.select("cust_id") \
            .exceptAll(vfr.select("cust_id")) \
            .join(cust_lng_lat, "cust_id")

        exist_df = vfr.join(cust_lng_lat, ["city", "cust_id"])

        if not_df.count()>0:
            #knn填充
            fill_df = fillWithKNN(exist_df.toPandas(), not_df.toPandas(), "avg_vfr")

            all_df = spark.createDataFrame(fill_df) \
                .unionByName(exist_df)
        else:
            all_df=exist_df

        #阈值
        threshold = all_df.groupBy("city") \
            .agg((f.mean("avg_vfr") + 3 * f.stddev_pop("avg_vfr")).alias("threshold"))

        truncate_df = all_df.join(threshold, "city") \
            .withColumn("avg_vfr", f.when(col("avg_vfr") > col("threshold"), col("threshold"))
                        .otherwise(col("avg_vfr"))
                        )

        log_df = truncate_df.withColumn("log", f.log(col("avg_vfr") + 1))
        log_max = log_df.groupBy("city").agg(f.max("log").alias("log_max"))

        colName = "people_count"
        log_df.join(log_max, "city") \
            .withColumn(colName, col("log") / col("log_max") * 5) \
            .foreachPartition(lambda x: write_hbase1(x, [colName], hbase))

        vfr.unpersist()
        cust_lng_lat.unpersist()
    except Exception:
        tb.print_exc()
# get_vfr_index()




#-----零售户店铺竞争指数
def get_cpt_index():
    # -----零售户店铺竞争指数
    # def get_cpt_index():
    print(f"{str(dt.now())}   cust competitive index")
    try:
        # 零售户档位
        co_cust = get_co_cust(spark).select("cust_id", "cust_seg") \
            .withColumn("cust_seg", f.regexp_replace(col("cust_seg"), "zz|ZZ", "31"))

        co_cust.cache()

        # 1.cust_id1 周围有cust_id0这些零售户
        # 2.零售户与范围内每个零售户的距离
        around_cust = get_around_cust(spark, 0.5)\
                                 .withColumn("length", haversine(col("lng1"), col("lat1"), col("lng0"),col("lat0"))) \
                                 .select("city1", "cust_id1", "cust_id0", "length")

        around_cust.cache()

        # 算出范围内距离的最大值和最小值
        max_min = around_cust.groupBy("city1", "cust_id1") \
            .agg(f.max("length").alias("max"), f.min("length").alias("min"))

        """
           距离系数=1/零售户与其他零售户距离的标准化后的值
           距离标准化值= ( 距离-min( 距离) )/(max( 距离 )-min( 距离 ))
        """
        # 距离系数
        dis_coe = around_cust.join(max_min, ["city1", "cust_id1"]) \
            .withColumn("dis_coe", 1 * (col("max") - col("min")) / (col("length") - col("min"))) \
            .select("city1", "cust_id1", "cust_id0", "dis_coe")

        # 1.获取cust_id1 档位 cust_seg1
        # 2.获取cust_id0 档位 cust_seg0
        # 3.档位差*距离系数
        # 4.加权平均档位差=sum((其他零售户档位-零售户档位)*距离系数)
        avg_seg_diff = dis_coe.join(co_cust, col("cust_id") == col("cust_id1")) \
            .drop("cust_id") \
            .withColumnRenamed("cust_seg", "cust_seg1") \
            .join(co_cust, col("cust_id") == col("cust_id0")) \
            .withColumnRenamed("cust_seg", "cust_seg0") \
            .groupBy("city1", "cust_id1") \
            .agg(f.sum((col("cust_seg0") - col("cust_seg1")) * col("dis_coe")).alias("avg_seg_diff")) \
            .select("city1", "cust_id1", "avg_seg_diff")

        # 零售户竞争强度=零售户500m范围加权平均档位差*范围内零售户数量
        cust_cpt = around_cust.groupBy("city1", "cust_id1").count() \
            .join(avg_seg_diff, ["city1", "cust_id1"]) \
            .withColumn("cust_cpt", col("avg_seg_diff") * col("count")) \
            .select("city1", "cust_id1", "cust_cpt")

        cust_cpt.cache()

        threshold = cust_cpt.groupBy("city1").agg(
            (f.mean("cust_cpt") + 3 * f.stddev_pop(col("cust_cpt"))).alias("threshold_plus"),
            (f.mean("cust_cpt") - 3 * f.stddev_pop(col("cust_cpt"))).alias("threshold_minus")
        )

        truncate_df = cust_cpt.join(threshold, "city1") \
            .withColumn("cust_cpt",
                        f.when(col("cust_cpt") > col("threshold_plus"), col("threshold_plus"))
                        .when(col("cust_cpt") < col("threshold_minus"), col("threshold_minus")) \
                        .otherwise(col("cust_cpt"))
                        )

        # 全市最大竞争强度
        max_cpt = truncate_df.groupBy("city1").agg(f.max(col("cust_cpt")).alias("max_cpt"),
                                                   f.min(col("cust_cpt")).alias("min_cpt"))

        # 店铺竞争指数
        colName = "order_competitive_index"
        result = truncate_df.join(max_cpt, "city1") \
            .withColumnRenamed("cust_id1", "cust_id") \
            .withColumn(colName, 5 - (col("cust_cpt") - col("min_cpt")) / (col("max_cpt") - col("min_cpt")) * 5) \
            .foreachPartition(lambda x: write_hbase1(x, [colName], hbase))

        co_cust.unpersist()
        around_cust.unpersist()
        cust_cpt.unpersist()
    except Exception:
        tb.print_exc()

# get_cpt_index()










# ------------------------------------------  retail（零售户分析）表 统计类信息--------------------------------



# get_order_stats_info()
def get_order_stats_info_monthly():
    # 某零售户上个月订货总量
    # 某零售户上个月的订货金额
    # 某零售户上个月的订货条均价
    # 某零售户上个月的订单数
    # 某零售户上个月订货总量环比变化情况
    # 某零售户上个月订货总金额环比变化情况
    # 某零售户上个月订货条均价环比变化情况
    co_co_01 = get_co_co_01(spark, scope=[0, 2], filter="month") \
        .select("cust_id", "qty_sum", "amt_sum", "month_diff")

    co_co_01.cache()

    # 上个月
    days = [1]
    cols0 = {
        "sum": ["sum_last_month"],
        "amount": ["amount_last_month"],
        "price": ["price_last_month"],
        "orders": ["orders_last_month"],

        "ring_sum": ["sum_ring_last_month"],
        "ring_amount": ["amount_ring_last_month"],
        "ring_price": ["price_ring_last_month"]
    }
    for i in range(len(days)):
        try:
            day = days[i]
            sum_colName = cols0["sum"][i]
            amount_colName = cols0["amount"][i]
            price_colName = cols0["price"][i]
            orders_colName = cols0["orders"][i]

            day_filter = co_co_01.where(col("month_diff") == day)

            print(f"{str(dt.now())}  上个月 订货总量")
            try:
                order_total = day_filter \
                    .groupBy("cust_id") \
                    .agg(f.sum("qty_sum").alias(sum_colName))
                order_total.foreachPartition(lambda x: write_hbase1(x, [sum_colName], hbase))
            except Exception:
                tb.print_exc()

            print(f"{str(dt.now())}  上个月 订货金额")
            try:
                amount_total = day_filter \
                    .groupBy("cust_id") \
                    .agg(f.sum("amt_sum").alias(amount_colName))
                amount_total.foreachPartition(lambda x: write_hbase1(x, [amount_colName], hbase))
            except Exception:
                tb.print_exc()

            print(f"{str(dt.now())}  上个月 订货条均价")
            try:
                avg_price = order_total.join(amount_total, "cust_id") \
                    .withColumn(price_colName, col(amount_colName) / col(sum_colName))
                avg_price.foreachPartition(lambda x: write_hbase1(x, [price_colName], hbase))
            except Exception:
                tb.print_exc()

            print(f"{str(dt.now())}  上个月 订单数")
            try:
                day_filter.groupBy("cust_id") \
                    .count() \
                    .withColumnRenamed("count", orders_colName) \
                    .foreachPartition(lambda x: write_hbase1(x, [orders_colName], hbase))
            except Exception:
                tb.print_exc()

            # -------------------------------------环比-----------------------------------
            # (1,2]
            day_filter = co_co_01.where(col("month_diff") == day + 1)

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
                .withColumn("ring_avg_price", col("ring_amt_sum") / col("ring_qty_ord"))

            ring_sum_colName = cols0["ring_sum"][i]
            ring_amount_colName = cols0["ring_amount"][i]
            ring_price_colName = cols0["ring_price"][i]

            print(f"{str(dt.now())}  上个月 订货总量环比变化情况")
            try:
                order_ring_ratio = ring_order_total.join(order_total, "cust_id") \
                    .withColumn(ring_sum_colName, period_udf(col(sum_colName), col("ring_qty_ord")))
                order_ring_ratio.foreachPartition(lambda x: write_hbase1(x, [ring_sum_colName], hbase))
            except Exception:
                tb.print_exc()

            print(f"{str(dt.now())}  上个月 订货总金额环比变化情况")
            try:
                amount_ring_ratio = ring_amount_total.join(amount_total, "cust_id") \
                    .withColumn(ring_amount_colName, period_udf(col(amount_colName), col("ring_amt_sum")))
                amount_ring_ratio.foreachPartition(lambda x: write_hbase1(x, [ring_amount_colName], hbase))
            except Exception:
                tb.print_exc()

            print(f"{str(dt.now())}  上个月 订货条均价环比变化情况")
            try:
                avg_price_ring_ratio = ring_avg_price.join(avg_price, "cust_id") \
                    .withColumn(ring_price_colName, period_udf(col(price_colName), col("ring_avg_price")))
                avg_price_ring_ratio.foreachPartition(lambda x: write_hbase1(x, [ring_price_colName], hbase))
            except Exception:
                tb.print_exc()


        except Exception:
            tb.print_exc()

    co_co_01.unpersist()


def get_order_stats_info_weekly():
    # 某零售户上一周/上两周/上四周订货总量
    # 某零售户上一周/上两周/上四周的订货金额
    # 某零售户上一周/上两周/上四周的订货条均价
    # 某零售户上一周/上两周/上四周的订单数
    # 某零售户上一周/上两周/上四周订货总量环比变化情况
    # 某零售户上一周/上两周/上四周订货总金额环比变化情况
    # 某零售户上一周/上两周/上四周订货条均价环比变化情况
    co_co_01 = get_co_co_01(spark, scope=[1, 5], filter="week") \
        .select("cust_id", "qty_sum", "amt_sum", "week_diff")

    co_co_01.cache()

    # 上一周/上两周/上四周
    days = [1, 2, 4]
    cols0 = {
        "sum": ["sum_last_week", "sum_last_two_week", "sum_last_four_week"],
        "amount": ["amount_last_week", "amount_last_two_week", "amount_last_four_week"],
        "price": ["price_last_week", "price_last_two_week", "price_last_four_week"],
        "orders": ["orders_last_week", "orders_last_two_week", "orders_last_four_week"],

        "ring_sum": ["sum_ring_last_week", "sum_ring_last_two_week", "sum_ring_last_four_week"],
        "ring_amount": ["amount_ring_last_week", "amount_ring_last_two_week", "amount_ring_last_four_week"],
        "ring_price": ["price_ring_last_week", "price_ring_last_two_week", "price_ring_last_four_week"]
    }
    for i in range(len(days)):
        try:
            day = days[i]
            sum_colName = cols0["sum"][i]
            amount_colName = cols0["amount"][i]
            price_colName = cols0["price"][i]
            orders_colName = cols0["orders"][i]

            day_filter = co_co_01.where(col("week_diff") <= day)

            print(f"{str(dt.now())}  上{day}周 订货总量")
            try:
                order_total = day_filter \
                    .groupBy("cust_id") \
                    .agg(f.sum("qty_sum").alias(sum_colName))
                order_total.foreachPartition(lambda x: write_hbase1(x, [sum_colName], hbase))
            except Exception:
                tb.print_exc()

            print(f"{str(dt.now())}  上{day}周 订货金额")
            try:
                amount_total = day_filter \
                    .groupBy("cust_id") \
                    .agg(f.sum("amt_sum").alias(amount_colName))
                amount_total.foreachPartition(lambda x: write_hbase1(x, [amount_colName], hbase))
            except Exception:
                tb.print_exc()

            print(f"{str(dt.now())}  上{day}周 订货条均价")
            try:
                avg_price = order_total.join(amount_total, "cust_id") \
                    .withColumn(price_colName, col(amount_colName) / col(sum_colName))
                avg_price.foreachPartition(lambda x: write_hbase1(x, [price_colName], hbase))
            except Exception:
                tb.print_exc()

            print(f"{str(dt.now())}  上{day}周 订单数")
            try:
                day_filter.groupBy("cust_id") \
                    .count() \
                    .withColumnRenamed("count", orders_colName) \
                    .foreachPartition(lambda x: write_hbase1(x, [orders_colName], hbase))
            except Exception:
                tb.print_exc()

            # -------------------------------------环比-----------------------------------
            # (1,2] (1,3] (1,5]
            day_filter = co_co_01.where((col("week_diff") > 1) & (col("week_diff") <= (1 + day)))

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
                .withColumn("ring_avg_price", col("ring_amt_sum") / col("ring_qty_ord"))

            ring_sum_colName = cols0["ring_sum"][i]
            ring_amount_colName = cols0["ring_amount"][i]
            ring_price_colName = cols0["ring_price"][i]

            print(f"{str(dt.now())}  上{day}周 订货总量环比变化情况")
            try:
                order_ring_ratio = ring_order_total.join(order_total, "cust_id") \
                    .withColumn(ring_sum_colName, period_udf(col(sum_colName), col("ring_qty_ord")))
                order_ring_ratio.foreachPartition(lambda x: write_hbase1(x, [ring_sum_colName], hbase))
            except Exception:
                tb.print_exc()

            print(f"{str(dt.now())}  上{day}周 订货总金额环比变化情况")
            try:
                amount_ring_ratio = ring_amount_total.join(amount_total, "cust_id") \
                    .withColumn(ring_amount_colName, period_udf(col(amount_colName), col("ring_amt_sum")))
                amount_ring_ratio.foreachPartition(lambda x: write_hbase1(x, [ring_amount_colName], hbase))
            except Exception:
                tb.print_exc()

            print(f"{str(dt.now())}  上{day}周 订货条均价环比变化情况")
            try:
                avg_price_ring_ratio = ring_avg_price.join(avg_price, "cust_id") \
                    .withColumn(ring_price_colName, period_udf(col(price_colName), col("ring_avg_price")))
                avg_price_ring_ratio.foreachPartition(lambda x: write_hbase1(x, [ring_price_colName], hbase))
            except Exception:
                tb.print_exc()
        except Exception:
            tb.print_exc()

    co_co_01.unpersist()





def get_order_yoy_info():
    #某零售户上个月订货总量同比变化情况
    #某零售户上个月订货总金额同比变化情况
    #某零售户上个月订货条均价同比变化情况
    colNames=["cust_id", "qty_sum", "amt_sum"]
    co_co_01 = get_co_co_01(spark,scope=[1,1],filter="month")\
                           .select(colNames)

    #-------------去年
    last_year = get_co_co_01(spark, scope=[13, 13], filter="month")\
                         .select(colNames)
    co_co_01.cache()
    last_year.cache()
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
                .withColumn("avg_price", col("amt_sum")/col("qty_sum"))
            # 去年同期
            last_year_avg_price = last_year_qty_sum.join(last_year_amt_sum, "cust_id") \
                .withColumn("last_year_avg_price", col("last_year_amt_sum")/col("last_year_qty_sum"))
            colName="price_lyear"
            print(f"{str(dt.now())}  某零售户上个月订货条均价同比变化情况  ", colName)
            last_year_avg_price.join(last_month_avg_price, "cust_id") \
                .withColumn(colName, period_udf(col("avg_price"), col("last_year_avg_price")))\
                .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))
        except Exception:
            tb.print_exc()

        co_co_01.unpersist()
        last_year.unpersist()
    except Exception:
        print(colName)
        tb.print_exc()

# get_order_yoy_info()




def get_ratio_monthly():
    # 某零售户上个月，上3个月，上6个月，上12个月所订省内烟、省外烟、进口烟的占比（省内外烟）
    # 零售户上个月，上3个月，上6个月，上12个月订购数前5省内烟
    # 零售户上个月，上3个月，上6个月，上12个月订购数前5省外烟
    # 某零售户上个月，上3个月，上6个月，上12个月所订卷烟价类占比
    # 某零售户上个月，上3个月，上6个月，上12个月所订卷烟价格分段占比
    try:
        co_co_line = get_co_co_line(spark, scope=[1, 12], filter="month") \
            .select("cust_id", "item_id", "qty_ord", "price", "month_diff")

        co_co_line.cache()

        plm_item = get_plm_item(spark).select("item_id", "yieldly_type", "kind", "item_name")

        line_plm = co_co_line.join(plm_item, "item_id")

        # 总量
        # 上个月
        total_month_1 = co_co_line.where(col("month_diff") <= 1) \
            .groupBy("cust_id") \
            .agg(f.sum("qty_ord").alias("qty_ord")).coalesce(10)
        # 上3个月
        total_month_3 = co_co_line.where(col("month_diff") <= 3) \
            .groupBy("cust_id") \
            .agg(f.sum("qty_ord").alias("qty_ord")).coalesce(10)
        # 上6个月
        total_month_6 = co_co_line.where(col("month_diff") <= 6) \
            .groupBy("cust_id") \
            .agg(f.sum("qty_ord").alias("qty_ord")).coalesce(10)
        # 上12个月
        total_month_12 = co_co_line.where(col("month_diff") <= 12) \
            .groupBy("cust_id") \
            .agg(f.sum("qty_ord").alias("qty_ord")).coalesce(10)

        days = {"1_month": 1, "3_month": 3, "6_month": 6, "12_month": 12}
        days_total = {"1_month": total_month_1, "3_month": total_month_3,
                      "6_month": total_month_6, "12_month": total_month_12}

        # 省内 0  省外 1 国外 3
        yieldly_types = ["0", "1", "3"]
        cols1 = {
            "1_month": ["in_prov_last_month", "out_prov_last_month", "import_last_month"],
            "3_month": ["in_prov_last_three_month", "out_prov_last_three_month", "import_last_three_month"],
            "6_month": ["in_prov_last_half_year", "out_prov_last_half_year", "import_last_half_year"],
            "12_month": ["in_prov_last_year", "out_prov_last_year", "import_last_year"]
        }
        # 省内 0 省外 1 的top5
        cols2 = {
            "1_month": ["in_prov_top5_last_month", "out_prov_top5_last_month"],
            "3_month": ["in_prov_top5_last_three_month", "out_prov_top5_last_three_month"],
            "6_month": ["in_prov_top5_last_half_year", "out_prov_top5_last_half_year"],
            "12_month": ["in_prov_top5_last_year", "out_prov_top5_last_year"]
        }
        # 1:一类,2:二类,3:三类,4:四类,5:五类,6:无价类
        kinds = ["1", "2", "3", "4", "5", "6"]
        cols3 = {
            "1_month": ["price_ratio_last_month_1", "price_ratio_last_month_2", "price_ratio_last_month_3",
                        "price_ratio_last_month_4", "price_ratio_last_month_5", "price_ratio_last_month_6"],
            "3_month": ["price_ratio_last_three_month_1", "price_ratio_last_three_month_2",
                        "price_ratio_last_three_month_3", "price_ratio_last_three_month_4",
                        "price_ratio_last_three_month_5", "price_ratio_last_three_month_6"],
            "6_month": ["price_ratio_last_half_year_1", "price_ratio_last_half_year_2", "price_ratio_last_half_year_3",
                        "price_ratio_last_half_year_4", "price_ratio_last_half_year_5", "price_ratio_last_half_year_6"],
            "12_month": ["price_ratio_last_year_1", "price_ratio_last_year_2", "price_ratio_last_year_3",
                         "price_ratio_last_year_4", "price_ratio_last_year_5", "price_ratio_last_year_6"]
        }
        # 50以下；50（含）-100，100（含）-300，300（含）-500，500（含）以上
        prices = [0, 50, 100, 300, 500]
        cols4 = {
            "1_month": ["price_sub_last_month_under50", "price_sub_last_month_50_to_100",
                        "price_sub_last_month_100_to_300", "price_sub_last_month_300_to_500",
                        "price_sub_last_month_up500"],
            "3_month": ["price_sub_last_three_month_under50", "price_sub_last_three_month_50_to_100",
                        "price_sub_last_three_month_100_to_300", "price_sub_last_three_month_300_to_500",
                        "price_sub_last_three_month_up500"],
            "6_month": ["price_sub_last_half_year_under50", "price_sub_last_half_year_50_to_100",
                        "price_sub_last_half_year_100_to_300", "price_sub_last_half_year_300_to_500",
                        "price_sub_last_half_year_up500"],
            "12_month": ["price_sub_last_year_under50", "price_sub_last_year_50_to_100",
                         "price_sub_last_year_100_to_300", "price_sub_last_year_300_to_500",
                         "price_sub_last_year_up500"]
        }

        win = Window.partitionBy("cust_id").orderBy(f.desc("yieldly_type_qty_ord"))

        for key in days.keys():
            # 对应日期的烟总量
            total_df = days_total[key]
            # 日期过滤条件
            day = days[key]

            day_filter = line_plm.where(col("month_diff") <= day)

            # -----某零售户上个月，上3个月，上6个月，上12个月所订省内烟、省外烟、进口烟的占比（省内外烟）
            for i in range(len(yieldly_types)):
                yieldly_type = yieldly_types[i]
                yieldly_type_filter = day_filter.where(col("yieldly_type") == yieldly_type)

                try:
                    colName = cols1[key][i]
                    print(f"{str(dt.now())}  yieldly_type:{yieldly_type} 上{day}个月省内烟、省外烟、进口烟的占比")
                    yieldly_type_filter.groupBy("cust_id") \
                        .agg(f.sum("qty_ord").alias("yieldly_type_qty_ord")) \
                        .join(total_df, "cust_id") \
                        .withColumn(colName, col("yieldly_type_qty_ord") / col("qty_ord")) \
                        .foreachPartition(lambda x: write_hbase1(x, [colName], hbase))
                except Exception:
                    tb.print_exc()

                # -----零售户上个月，上3个月，上6个月，上12个月订购数前5省内烟
                # -----零售户上个月，上3个月，上6个月，上12个月订购数前5省外烟

                if yieldly_type in ["0", "1"]:
                    try:
                        print(f"{str(dt.now())}   yieldly_type:{yieldly_type} 上{day}个月省内烟、省外烟的top5")
                        colName = cols2[key][i]
                        top5 = yieldly_type_filter.groupBy("cust_id", "item_id") \
                            .agg(f.sum("qty_ord").alias("yieldly_type_qty_ord")) \
                            .withColumn("rank", f.row_number().over(win)) \
                            .where(col("rank") <= 5)
                        top5.join(plm_item, "item_id") \
                            .groupBy("cust_id") \
                            .agg(f.collect_list("item_name").alias(colName)) \
                            .foreachPartition(lambda x: write_hbase1(x, [colName], hbase))
                    except Exception:
                        tb.print_exc()

            # -----某零售户上个月，上3个月，上6个月，上12个月所订卷烟价类占比
            for i in range(len(kinds)):
                kind = kinds[i]
                kind_filter = day_filter.where(col("kind") == kind)
                colName = cols3[key][i]
                print(f"{str(dt.now())}  kind:{kind} 上{day}个月卷烟价类占比")
                try:
                    kind_filter.groupBy("cust_id") \
                        .agg(f.sum("qty_ord").alias("kind_qty_ord")) \
                        .join(total_df, "cust_id") \
                        .withColumn(colName, col("kind_qty_ord") / col("qty_ord")) \
                        .foreachPartition(lambda x: write_hbase1(x, [colName], hbase))

                except Exception:
                    tb.print_exc()

            # -----某零售户上个月，上3个月，上6个月，上12个月所订卷烟价格分段占比
            for i in range(len(prices)):
                try:
                    price = prices[i]
                    colName = cols4[key][i]
                    if price == 500:
                        price_filter = day_filter.where(col("price") >= price)
                        print(f"{str(dt.now())}  price:({price},∞],上{day}个月卷烟价格分段占比")
                    else:
                        price_filter = day_filter.where((col("price") >= price) & (col("price") < prices[i + 1]))
                        print(f"{str(dt.now())}  price:({price},{prices[i+1]}],上{day}个月卷烟价格分段占比")

                    price_filter.groupBy("cust_id") \
                        .agg(f.sum("qty_ord").alias("price_qty_ord")) \
                        .join(total_df, "cust_id", "left") \
                        .withColumn(colName, col("price_qty_ord") / col("qty_ord")) \
                        .foreachPartition(lambda x: write_hbase1(x, [colName], hbase))
                except Exception:
                    tb.print_exc()

        co_co_line.unpersist()
    except Exception:
        tb.print_exc()


def get_ratio_weekly():
    # 某零售户上1周所订省内烟、省外烟、进口烟的占比（省内外烟）
    # 零售户上1周订购数前5省内烟
    # 零售户上1周订购数前5省外烟
    # 某零售户上1周所订卷烟价类占比
    # 某零售户上1周所订卷烟价格分段占比
    try:
        co_co_line = get_co_co_line(spark, scope=[1, 1], filter="week") \
            .select("cust_id", "item_id", "qty_ord", "price","week_diff")

        co_co_line.cache()

        plm_item = get_plm_item(spark).select("item_id", "yieldly_type", "kind", "item_name")

        line_plm = co_co_line.join(plm_item, "item_id")

        # 上1周总量
        total_week_1 = co_co_line.groupBy("cust_id").agg(f.sum("qty_ord").alias("qty_ord"))

        days = {"1_week": 1}
        days_total = {"1_week": total_week_1}

        # 省内 0  省外 1 国外 3
        yieldly_types = ["0", "1", "3"]
        cols1 = {
            "1_week": ["in_prov_last_week", "out_prov_last_week", "import_last_week"]
        }
        # 省内 0 省外 1 的top5
        cols2 = {
            "1_week": ["in_prov_top5_last_week", "out_prov_top5_last_week"]
        }
        # 1:一类,2:二类,3:三类,4:四类,5:五类,6:无价类
        kinds = ["1", "2", "3", "4", "5", "6"]
        cols3 = {
            "1_week": ["price_ratio_last_week_1", "price_ratio_last_week_2", "price_ratio_last_week_3",
                       "price_ratio_last_week_4", "price_ratio_last_week_5", "price_ratio_last_week_6"]
        }
        # 50以下；50（含）-100，100（含）-300，300（含）-500，500（含）以上
        prices = [0, 50, 100, 300, 500]
        cols4 = {
            "1_week": ["price_sub_last_week_under50", "price_sub_last_week_50_to_100", "price_sub_last_week_100_to_300",
                       "price_sub_last_week_300_to_500", "price_sub_last_week_up500"]
        }

        win = Window.partitionBy("cust_id").orderBy(f.desc("yieldly_type_qty_ord"))

        for key in days.keys():
            # 对应日期的烟总量
            total_df = days_total[key]
            # 日期过滤条件
            day = days[key]

            day_filter = line_plm.where(col("week_diff") == day)

            # -----某零售户上1周所订省内烟、省外烟、进口烟的占比（省内外烟）
            for i in range(len(yieldly_types)):
                yieldly_type = yieldly_types[i]
                yieldly_type_filter = day_filter.where(col("yieldly_type") == yieldly_type)

                try:
                    colName = cols1[key][i]
                    print(f"{str(dt.now())}  yieldly_type:{yieldly_type},上一周省内、省外、进口烟的占比")
                    yieldly_type_filter.groupBy("cust_id") \
                        .agg(f.sum("qty_ord").alias("yieldly_type_qty_ord")) \
                        .join(total_df, "cust_id") \
                        .withColumn(colName, col("yieldly_type_qty_ord") / col("qty_ord")) \
                        .foreachPartition(lambda x: write_hbase1(x, [colName], hbase))
                except Exception:
                    tb.print_exc()

                # -----零售户上1周订购数前5省内烟
                # -----零售户上1周订购数前5省外烟
                if yieldly_type in ["0", "1"]:
                    try:
                        print(f"{str(dt.now())}  yieldly_type:{yieldly_type},上一周top5")
                        colName = cols2[key][i]
                        top5 = yieldly_type_filter.groupBy("cust_id", "item_id") \
                            .agg(f.sum("qty_ord").alias("yieldly_type_qty_ord")) \
                            .withColumn("rank", f.row_number().over(win)) \
                            .where(col("rank") <= 5)
                        top5.join(plm_item, "item_id") \
                            .groupBy("cust_id") \
                            .agg(f.collect_list("item_name").alias(colName)) \
                            .foreachPartition(lambda x: write_hbase1(x, [colName], hbase))
                    except Exception:
                        tb.print_exc()

            # -----某零售户上1周所订卷烟价类占比
            for i in range(len(kinds)):
                kind = kinds[i]
                kind_filter = day_filter.where(col("kind") == kind)
                colName = cols3[key][i]
                print(f"{str(dt.now())}  kind:{kind},上一周卷烟价类占比")
                try:
                    kind_filter.groupBy("cust_id") \
                        .agg(f.sum("qty_ord").alias("kind_qty_ord")) \
                        .join(total_df, "cust_id") \
                        .withColumn(colName, col("kind_qty_ord") / col("qty_ord")) \
                        .foreachPartition(lambda x: write_hbase1(x, [colName], hbase))

                except Exception:
                    tb.print_exc()

            # -----某零售户上1周所订卷烟价格分段占比
            for i in range(len(prices)):
                try:
                    price = prices[i]
                    colName = cols4[key][i]
                    if price == 500:
                        price_filter = day_filter.where(col("price") >= price)
                        print(f"{str(dt.now())}  price:({price},∞],上一周卷烟价格分段占比")
                    else:
                        price_filter = day_filter.where((col("price") >= price) & (col("price") < prices[i + 1]))
                        print(f"{str(dt.now())}  price:({price},{prices[i+1]}],上一周卷烟价格分段占比")

                    price_filter.groupBy("cust_id") \
                        .agg(f.sum("qty_ord").alias("price_qty_ord")) \
                        .join(total_df, "cust_id", "left") \
                        .withColumn(colName, col("price_qty_ord") / col("qty_ord")) \
                        .foreachPartition(lambda x: write_hbase1(x, [colName], hbase))
                except Exception:
                    tb.print_exc()

        co_co_line.unpersist()
    except Exception:
        tb.print_exc()
