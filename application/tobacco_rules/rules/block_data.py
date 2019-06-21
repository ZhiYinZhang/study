#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/5/23 11:37
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as f
from application.tobacco_rules.rules.utils import *
from application.tobacco_rules.rules.write_hbase import  write_hbase1


spark = SparkSession.builder.enableHiveSupport().appName("block_data").getOrCreate()
sc=spark.sparkContext
sc.setLogLevel("WARN")

spark.sql("use aistrong")


#---------城市分块统计数据
hbase={"table":"TOBACCO.BLOCK_DATA","families":["0"],"row":"block_id"}

#区域卷烟主销品规（省内、省外烟）
def get_block_item_top():
    print(f"{str(dt.now())} 区域卷烟主销品规")
    try:
        # 岳阳市分块数据
        block_data = spark.read.csv("/user/entrobus/tobacco_data/city_block_data/岳阳市-3km-blockData.csv", header=True) \
                                .withColumnRenamed("ind(第几个)", "block_id") \
                                .withColumnRenamed("southWestLng(西南-左下)", "lng1") \
                                .withColumnRenamed("southWestLat(西南-左下)", "lat1") \
                                .withColumnRenamed("northEastLng(东北-右上)", "lng0") \
                                .withColumnRenamed("northEastLat(东北-右上)", "lat0") \
                                .select("block_id", "lng1", "lat1", "lng0", "lat0")

        # 零售户经纬度
        cust_lng_lat = get_cust_lng_lat(spark).where(col("city") == "岳阳市") \
            .select("cust_id", "lng", "lat")

        # 省内 省外
        plm_item = get_plm_item(spark).where(col("yieldly_type").isin(["0", "1"]))\
                                      .select("item_id", "item_name","yieldly_type")


        provs=["0","1"]
        cols=["block_in_prov_top5","block_out_prov_top5"]
        for i in range(len(provs)):
            prov=provs[i]
            colName=cols[i]

            print(f"{str(dt.now())}  {i}")
            # qty_ord 实际订购量    qty_rsn 合理订购量
            co_co_line = get_co_co_line(spark, [0, 30]).where((col("com_id") == "011114306") & (col("qty_ord") != 0)) \
                                                    .join(plm_item.where(col("yieldly_type") == prov), "item_id") \
                                                    .select("cust_id", "item_id", "qty_ord", "qty_rsn")

            # 每个零售户各类烟的订单量
            cust_item_qty = co_co_line.groupBy("cust_id", "item_id") \
                .agg(f.sum("qty_ord").alias("qty_ord"), f.sum("qty_rsn").alias("qty_rsn"))


            # 归一化
            # 每个零售户所定烟的订单量的最大值和最小值
            max_min = cust_item_qty.groupBy("cust_id")\
                                  .agg(f.max("qty_ord").alias("ord_max"), f.min("qty_ord").alias("ord_min"),
                                       f.max("qty_rsn").alias("rsn_max"), f.min("qty_rsn").alias("rsn_min"))

            item_qty_std = cust_item_qty.join(max_min, "cust_id") \
                .withColumn("item_qty_ord_std", (col("qty_ord") - col("ord_min")) / (col("ord_max") - col("ord_min"))) \
                .withColumn("item_qty_rsn_std", (col("qty_rsn") - col("rsn_min")) / (col("rsn_max") - col("rsn_min")))


            #每个区块的零售户 烟
            block_item = block_data.join(cust_lng_lat, (col("lng") > col("lng1")) & (col("lng") < col("lng0")) & (
                                                      col("lat") > col("lat1")) & (col("lat") < col("lat0"))) \
                                    .join(item_qty_std, "cust_id") \
                                    .select("block_id", "cust_id", "item_id", "item_qty_ord_std", "item_qty_rsn_std")


            # 1.每个区域各卷烟的销量
            block_item_num = block_item.groupBy("block_id", "item_id") \
                                        .agg(f.sum("item_qty_ord_std").alias("block_item_ord_num"),
                                             f.sum("item_qty_rsn_std").alias("block_item_rsn_num"))

            # 2.每个区域 卷烟店铺覆盖率  在规定区域内，每款卷烟覆盖的零售户店面数量/总零售户店面数量。
            block_total_cust_num = block_item.groupBy("block_id")\
                                             .agg(f.count("cust_id").alias("block_total_cust_num"))
            coverage_rate = block_item.groupBy("block_id", "item_id") \
                                        .agg(f.count("cust_id").alias("block_cust_num")) \
                                        .join(block_total_cust_num, "block_id") \
                                        .withColumn("coverage_rate", col("block_cust_num") / col("block_total_cust_num")) \
                                        .select("block_id", "item_id", "coverage_rate")

            # 3.每个区域 卷烟占比  基于 1   在规定的区域内，每款卷烟销量/所有卷烟总销量的比率。
            block_total_item_num = block_item.groupBy("block_id") \
                                             .agg(f.sum("item_qty_ord_std").alias("block_total_item_num"))
            item_ratio = block_item_num.join(block_total_item_num, "block_id") \
                                        .withColumn("item_ratio", col("block_item_ord_num") / col("block_total_item_num")) \
                                        .select("block_id", "item_id", "item_ratio")


            # 4.每个区域 卷烟订足率   基于 1   在规定的区域内，各卷烟实际订购量/该卷烟合理订购量。
            enough_rate = block_item_num.withColumn("enough_rate", col("block_item_ord_num") / col("block_item_rsn_num")) \
                                        .select("block_id", "item_id", "enough_rate")


            # 总分=0.4*卷烟占比+0.4*卷烟订足率+0.2*卷烟店铺覆盖率
            score = coverage_rate.join(item_ratio, ["block_id", "item_id"]) \
                                .join(enough_rate, ["block_id", "item_id"]) \
                                .withColumn("score", col("item_ratio") * 0.4 + col("enough_rate") * 0.4 + col("coverage_rate") * 0.2) \
                                .select("block_id", "item_id", "score")


            # 规则1   挑选卷烟订足率前100，并且卷烟店铺覆盖率前100的卷烟。
            win1 = Window.partitionBy("block_id").orderBy(col("enough_rate").desc())
            win2 = Window.partitionBy("block_id").orderBy(col("coverage_rate").desc())
            enough_rate_rank = enough_rate.withColumn("rank", f.row_number().over(win1)) \
                                        .where(col("rank") <= 100) \
                                        .select("block_id", "item_id")
            coverage_rate_rank = coverage_rate.withColumn("rank", f.row_number().over(win2)) \
                                            .where(col("rank") <= 100) \
                                            .select("block_id", "item_id")
            rule1 = enough_rate_rank.join(coverage_rate_rank, ["block_id", "item_id"])

            # 规则2：计算基于规则1挑选出来的卷烟的总分，总分排名前5款卷烟即为该区域主销卷烟。
            win = Window.partitionBy("block_id").orderBy(col("score").desc())

            top5 = rule1.join(score, ["block_id", "item_id"]) \
                        .withColumn("rank", f.row_number().over(win)) \
                        .where(col("rank") <= 5) \
                        .select("block_id", "item_id") \
                        .join(plm_item, "item_id") \
                        .groupBy("block_id") \
                        .agg(f.collect_list("item_name").alias(colName))

            top5.foreachPartition(lambda x:write_hbase1(x,[colName],hbase))
    except Exception:
        tb.print_exc()


# get_block_item_top()



