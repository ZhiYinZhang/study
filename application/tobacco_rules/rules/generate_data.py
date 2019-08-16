#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/5/23 17:45
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as f
from pyspark.sql import Window
from datetime import datetime as dt
import datetime
import os
import traceback as tb
from rules.utils import lng_l,lng_r,lat_u,lat_d,get_cust_lng_lat,get_consume_level,haversine,get_rating,get_co_cust,get_vfr
from rules.config import avg_vfr_path,cons_level_path,retail_table,\
    cluster_path,cigar_rating_path,cigar_property_path
from ml.cluster import main

spark = SparkSession.builder\
                    .enableHiveSupport().appName("generate data").getOrCreate()
spark.sql("use aistrong")
sc=spark.sparkContext
sc.setLogLevel("WARN")

def generate_near_cust():
    """
      已移除
      生成距离零售户最近的同市100个零售户 存入hdfs
      YJFL001指标
      更新:零售户经纬度数据变化
      用时40m  driver-memory 10g executor-memory 20g num-executors 5 executor-cores 4
    :param spark:
    :return:
    """
    print(f"{str(dt.now())} 生成距离零售户最近的同市100个零售户")
    try:
        cust_lng_lat = get_cust_lng_lat(spark)\
                        .withColumnRenamed("lng", "lng0") \
                        .withColumnRenamed("lat", "lat0") \
                        .withColumnRenamed("city", "city0") \
                        .withColumnRenamed("cust_id","cust_id0")\
                        .select("cust_id0", "lng0", "lat0", "city0")
        cust_lng_lat1 = cust_lng_lat\
                            .withColumnRenamed("lng0", "lng1") \
                            .withColumnRenamed("lat0", "lat1") \
                            .withColumnRenamed("cust_id0", "cust_id1") \
                            .withColumnRenamed("city0", "city1")

        cities = ["岳阳市", "邵阳市", "株洲市"]
        path = ["yueyang", "shaoyang", "zhuzhou"]


        # 零售户cust_id1 最近100个零售户 cust_id0
        win = Window.partitionBy("cust_id1").orderBy(col("distance"))

        for i in range(len(cities)):
            city = cities[i]
            print(f"{str(dt.now())}", city)
            result = cust_lng_lat.where(col("city0") == city) \
                .crossJoin(cust_lng_lat1.where(col("city1") == city)) \
                .withColumn("distance", haversine(col("lng0"), col("lat0"), col("lng1"), col("lat1")))

            result.withColumn("rank", f.row_number().over(win)) \
                .where(col("rank") <= 100) \
                .write.csv(header=True, path="/user/entrobus/tobacco_data/cust_cross_join/" + path[i], mode="overwrite")
    except Exception:
        tb.print_exc()


def generate_around_vfr(around):
    """
    零售户周边人流数 =零售户半径{around}km范围内平均人流
    平均人流=近30天所有记录中距离中心点最近的100个观测记录的均值（距离不超过{around}km，若不足100个则有多少算多少）
    更新:人流数据变化/零售户经纬度
    用时:23m  driver-memory 10g executor-memory 20g num-executors 5 executor-cores 4
    """
    print(f"{str(dt.now())} 零售户周边人流数")
    try:
        #人流数据
        vfr=get_vfr(spark).groupBy("city","wgs_lng","wgs_lat")\
                          .agg(f.sum("count").alias("count"))
        co_cust=get_co_cust(spark).select("cust_id")
        cust_lng_lat=get_cust_lng_lat(spark).join(co_cust,"cust_id")\
                                        .withColumn("scope", f.lit(around)) \
                                        .withColumn("lng_l", lng_l(col("lng"), col("lat"), col("scope"))) \
                                        .withColumn("lng_r", lng_r(col("lng"), col("lat"), col("scope"))) \
                                        .withColumn("lat_d", lat_d(col("lat"), col("scope"))) \
                                        .withColumn("lat_u", lat_u(col("lat"), col("scope"))) \
                                        .select("city", "cust_id", "lng_l", "lng_r", "lat_d", "lat_u", "lng", "lat")

        cust_lng_lat.cache()
        vfr.cache()

        win = Window.partitionBy(col("cust_id")).orderBy(col("length"))

        cities = ["邵阳市", "岳阳市", "株洲市"]
        for city in cities:
            try:
                print(f"{str(dt.now())} {city}")
                cust_lng_lat0 = cust_lng_lat.where(col("city") == city)
                vfr0=vfr.where(col("city")==city).drop("city")

                # 零售户周边人流数=零售户半径500m范围内平均人流
                avg_vfr = cust_lng_lat0.join(vfr0, (col("wgs_lng") >= col("lng_l")) & (col("wgs_lng") <= col("lng_r")) &
                                                   (col("wgs_lat") >= col("lat_d")) & (col("wgs_lat") <= col("lat_u"))) \
                                        .withColumn("length", haversine(col("lng"), col("lat"), col("wgs_lng"), col("wgs_lat"))) \
                                        .withColumn("rank", f.row_number().over(win)) \
                                        .where(col("rank") <= 100) \
                                        .groupBy("city", "cust_id") \
                                        .agg(f.avg(col("count")).alias("avg_vfr"))
                avg_vfr.write.csv(header=True, path=os.path.join(avg_vfr_path, city), mode="overwrite")

            except Exception:
                tb.print_exc()

        cust_lng_lat.unpersist()
        vfr.unpersist()
    except:
        tb.print_exc()
# generate_around_vfr(0.5)


def generate_all_cust_cons():
    """
      得到所有零售户消费水平:根据有消费水平的零售户cust_id0 去 生成没有消费水平的零售户cust_id1的消费水平
      更新:零售户经纬度/[租金,餐饮,酒店]变化
      用时:11m  driver-memory 10g executor-memory 20g num-executors 5 executor-cores 4
      1.获取没有消费水平的零售户
      2.获取没有消费水平的零售户的经纬度
      3.获取有消费水平的零售户的经纬度
      4.两个DataFrame进行crossJoin
      5.计算cust_id1,cust_id0的距离
      6.取距离cust_id1最近的30个cust_id0
      7.求这30个cust_id0的消费水平的均值作为cust_id1的消费水平
    :param spark:
    :return:
    """
    print(f"{str(dt.now())} 生成零售户消费水平")
    try:
        # 零售户经纬度
        cust_lng_lat = get_cust_lng_lat(spark)\
                              .select("city", "cust_id", "lng", "lat")

        # 有消费水平的零售户
        consume_level_df = get_consume_level(spark) \
                                  .select("city","cust_id", "consume_level")

        # 1.
        not_cons_cust = cust_lng_lat.select("cust_id").exceptAll(consume_level_df.select("cust_id"))
        # 2.
        not_cons_cust = not_cons_cust.join(cust_lng_lat, "cust_id") \
                                    .withColumnRenamed("cust_id", "cust_id1") \
                                    .withColumnRenamed("lng", "lng1") \
                                    .withColumnRenamed("lat", "lat1") \
                                    .withColumnRenamed("city", "city1")

        # 3.
        cons_cust = consume_level_df.join(cust_lng_lat, ["city","cust_id"]) \
                                    .withColumnRenamed("cust_id", "cust_id0") \
                                    .withColumnRenamed("lng", "lng0") \
                                    .withColumnRenamed("lat", "lat0")
        # 4. 5. 6.
        win = Window.partitionBy("cust_id1").orderBy("distance")
        nearest_30 = not_cons_cust.crossJoin(cons_cust) \
                                .withColumn("distance", haversine(col("lng1"), col("lat1"), col("lng0"), col("lat0"))) \
                                .withColumn("rank", f.row_number().over(win)) \
                                .where(col("rank") <= 30) \
                                .select("city1", "cust_id1", "consume_level")
        # 7.
        result = nearest_30.groupBy("city1", "cust_id1") \
                            .agg(f.avg("consume_level").alias("consume_level")) \
                            .withColumnRenamed("cust_id1", "cust_id") \
                            .withColumnRenamed("city1", "city")


        result.unionByName(consume_level_df)\
                .write.csv(header=True,path=cons_level_path,mode="overwrite")
    except Exception:
        tb.print_exc()


# generate_all_cust_cons()


def generate_cust_cluster():
    #生成零售户聚类结果

    cities=["邵阳市","岳阳市","株洲市"]
    #零售户画像表
    phoenix_table=retail_table
    for city in cities:
        print(f"{str(dt.now())} {city}零售户聚类结果")
        main(city,phoenix_table,cluster_path)


def generate_cigar_rating():
    #每个零售户对每款烟的评分
    try:
        print(f"{str(dt.now())} 生成每个零售户对每品规烟的评分")
        cigar_rate=get_rating(spark, "item_id")

        cigar_rate.write.parquet(cigar_rating_path,mode="overwrite")
    except:
        tb.print_exc()




from rules.utils import get_plm_item,element_at,item_name_udf
from rules.write_hbase import write_hbase1
from rules.config import brand_table,cigar_static_table
def generate_brand_data():
    try:

        hbase = {"table": brand_table, "families": ["0"], "row": "brand_id"}
        print(f"{dt.now()} 生成品牌名称 {brand_table}")

        #写入品牌表
        plm_item=get_plm_item(spark).select("item_id","item_name","item_kind","is_mrb")
        # item_kind !=4 4为罚没  is_mrb=1 1为在使用   去掉空格  去掉*   中文括号用英文括号替换
        #处理item_name
        #??? 可以用所有数据 然后drop掉没有括号的
        plm_item_brand = plm_item\
                    .where(col("item_kind") != "4") \
                    .where(col("is_mrb") == "1") \
                    .withColumn("brand_name",item_name_udf(col("item_name")))\
                    .dropDuplicates(["brand_name"]) \
                    .where(col("brand_name") != "")

        from pyspark.sql import Window
        win = Window.orderBy("item_id")
        brand=plm_item_brand.select("item_id", "item_name", "brand_name") \
                       .withColumn("brand_id", f.row_number().over(win))
        brand.foreachPartition(lambda x:write_hbase1(x,["brand_name"],hbase))
    except:
        tb.print_exc()


def generate_cigar_static_data():
    #按照现有规则过滤的烟 只有 127款烟
    try:
        hbase = {"table": cigar_static_table, "families": ["0"], "row": "gauge_id"}
        print(f"{dt.now()} 卷烟静态数据 {cigar_static_table}")

        #品牌拥有者
        brand_owner=spark.sql("select * from DB2_DB2INST1_PLM_BRANDOWNER where dt=(select max(dt) from DB2_DB2INST1_PLM_BRANDOWNER)") \
                            .select("brdowner_id", "brdowner_name")

        #卷烟策略类型   ?不同公司 同一款烟 策略不一样
        plm_item_com = spark.sql(
            "select * from DB2_DB2INST1_PLM_ITEM_COM where dt=(select max(dt) from DB2_DB2INST1_PLM_ITEM_COM)") \
                .where(col("com_id")=='011114302')\
                .select("item_id","price_trade","tactic_type")

        #取co_co_line 株洲市 近60天有订单数据的烟
        date = str((dt.now() - datetime.timedelta(60)).date())
        co_co_line = spark.sql(f"select com_id,item_id from DB2_DB2INST1_CO_CO_LINE where dt>'{date}'") \
                                                .where(col("com_id") == "011114302")




        plm_item = get_plm_item(spark) \
                         .where((col("is_mrb") == "1") & (col("item_kind") != "4")) \
                         .join(brand_owner,"brdowner_id")\
                         .join(plm_item_com,"item_id")\
                         .join(co_co_line,"item_id")


        # item_kind !=4 4为罚没   去掉*   去掉空格   中文括号用英文括号替换
        plm_item_etl = plm_item.select("brdowner_name","item_id", "item_name","tactic_type",
                                      "kind", "yieldly_type","product_type", "is_thin","price","is_middle","is_short") \
                                .withColumn("item_name_etl", f.trim(col("item_name"))) \
                                .withColumn("item_name_etl", f.regexp_replace(col("item_name_etl"), "\*|^f|^F", "")) \
                                .withColumn("item_name_etl", f.regexp_replace(col("item_name_etl"), "\（", "\(")) \
                                .withColumn("item_name_etl", f.regexp_replace(col("item_name_etl"), "\）", "\)"))


        waibu = spark.read.csv(cigar_property_path, header=True)

        cigar_static=plm_item_etl.join(waibu, col("item_name_etl") == col("name")) \
                    .withColumnRenamed("brdowner_name","industrial_company")\
                    .withColumnRenamed("item_id","gauge_id") \
                    .withColumnRenamed("item_name", "gauge_name") \
                    .withColumnRenamed("tactic_type","ciga_policy_type")\
                    .withColumnRenamed("price","ciga_price")\
                    .withColumnRenamed("kind", "ciga_class") \
                    .withColumnRenamed("product_type", "ciga_type") \
                    .withColumnRenamed("yieldly_type", "ciga_origin_type") \
                    .withColumnRenamed("cigar_long", "ciga_long")

        cols=["industrial_company","brand_name","gauge_name","ciga_policy_type","ciga_class","ciga_price","ciga_origin_type","ciga_type","is_thin"
              ,"tar_cont","gas_nicotine","co_cont","m_long","ciga_long","package_type","box_num","sales_type","is_middle","is_short"]
        cigar_static.foreachPartition(lambda x:write_hbase1(x,cols,hbase))

    except:
        tb.print_exc()
