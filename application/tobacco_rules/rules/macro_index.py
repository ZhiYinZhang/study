#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/5/20 17:51
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as f
from rules.utils import *
from rules.write_hbase import  write_hbase1
from rules.config import data_index_table

"""
地区宏观经济指标
"""

spark = SparkSession.builder.enableHiveSupport().appName("macro_index").getOrCreate()
sc=spark.sparkContext
sc.setLogLevel("WARN")

spark.sql("use aistrong")

hbase={"table":data_index_table,"families":["0"],"row":"sale_center_id"}




# 城市 city   县或区  county
def get_city():
    print(f"{str(dt.now())} city county")
    try:
        get_area(spark).select("city","county","sale_center_id")\
                  .groupBy("city","sale_center_id").agg(f.collect_list("county").alias("county"))\
                  .foreachPartition(lambda x:write_hbase1(x,["city","county","sale_center_id"],hbase))
    except Exception:
        tb.print_exc()






def get_area_info():
    print(f"{str(dt.now())}   gdp ...")
    try:
        area_code = get_area(spark).select("county", "sale_center_id")

        population = get_city_ppl(spark).select("区县", "城镇人口", "农村人口")

        cols = ["county", "gdp", "gdp_add", "consumption", "consumption_add", "farmer", "farmer_add", "urban",
                "urban_add",
                "primary_industry", "primary_industry_add",
                "second_industry", "second_industry_add",
                "tertiary_industry", "tertiary_industry_add"]

        city_info = get_city_info(spark).where(col("mtime") == f"{dt.now().year-1}-12-01") \
            .select(cols) \
            .join(population, col("county") == col("区县")) \
            .join(area_code, "county")

        # 计算每个营销中心的总城镇人口和总农村人口
        total_ppl = city_info.groupBy("sale_center_id") \
            .agg(f.sum("城镇人口").alias("urban_total_ppl"), f.sum("农村人口").alias("farmer_total_ppl"))

        # 计算每个区县的城镇人口 占 整个营销中心总城镇人口的比值
        # 计算每个区县的农村人口 占 整个营销中心总农村人口的比值
        weight = city_info.join(total_ppl, "sale_center_id") \
            .withColumn("urban_weight", col("城镇人口") / col("urban_total_ppl")) \
            .withColumn("farmer_weight", col("农村人口") / col("farmer_total_ppl"))

        # urban urban_add farmer farmer_add 加权平均
        result = weight.groupBy("sale_center_id") \
                        .agg(
                        f.sum("gdp").alias("gdp"),
                        f.avg("gdp_add").alias("gdp_add"),

                        f.sum("consumption").alias("consumption"),
                        f.avg("consumption_add").alias("consumption_add"),

                        f.sum(col("urban") * col("urban_weight")).alias("urban"),
                        f.sum(col("urban_add") * col("urban_weight")).alias("urban_add"),

                        f.sum(col("farmer") * col("farmer_weight")).alias("farmer"),
                        f.sum(col("farmer_add") * col("farmer_weight")).alias("farmer_add"),

                        f.sum("primary_industry").alias("primary_industry"),
                        f.avg("primary_industry_add").alias("primary_industry_add"),

                        f.sum("second_industry").alias("second_industry"),
                        f.avg("second_industry_add").alias("second_industry_add"),

                        f.sum("tertiary_industry").alias("tertiary_industry"),
                        f.avg("tertiary_industry_add").alias("tertiary_industry_add")
                      )
        cols.remove("county")
        result.foreachPartition(lambda x: write_hbase1(x, cols, hbase))
    except Exception:
        tb.print_exc()





def get_area_ppl():
    print(f"{str(dt.now())}   人口数量")
    try:
        population = get_city_ppl(spark).select( "区县", "总人口", "常住人口", "城镇人口", "农村人口")
        area_code = get_area(spark).where(col("city").rlike("株洲市|邵阳市|岳阳市")) \
                                  .select("city","county", "sale_center_id")
        area_plt = population.join(area_code, col("county") == col("区县"))

        cols=["total_number","permanent_number","urban_number","country_number"]

        # 每个区域中心的人口
        sale_center_ppl = area_plt.groupBy("city", "sale_center_id") \
                                  .agg(f.sum("总人口").alias(cols[0]), f.sum("常住人口").alias(cols[1]),
                                         f.sum("城镇人口").alias(cols[2]), f.sum("农村人口").alias(cols[3]))

        sale_center_ppl.foreachPartition(lambda x:write_hbase1(x,cols,hbase))
    except Exception:
        tb.print_exc()







