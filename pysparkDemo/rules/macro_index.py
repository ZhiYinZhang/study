#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/5/20 17:51
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as f
from pysparkDemo.rules.utils import *
from pysparkDemo.rules.write_hbase import  write_hbase1



spark = SparkSession.builder.enableHiveSupport().appName("macro_index").getOrCreate()
sc=spark.sparkContext
sc.setLogLevel("WARN")

spark.sql("use aistrong")

#---------地区宏观经济指标
hbase={"table":"TOBACCO.DATA_INDEX","families":["0"],"row":"sale_center_id"}




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

        cols = ["county", "gdp","gdp_add", "consumption", "farmer", "urban", "primary_industry", "primary_industry_add",
                "second_industry", "second_industry_add", "tertiary_industry", "tertiary_industry_add"]

        result = get_city_info(spark).where(col("mtime") == f"{dt.now().year-1}-12-01") \
            .select(cols) \
            .join(area_code, "county") \
            .groupBy("sale_center_id") \
            .agg(f.sum("gdp").alias("gdp"),f.avg("gdp_add").alias("gdp_add"),
                 f.sum("consumption").alias("consumption"), f.sum("farmer").alias("farmer"),
                 f.sum("urban").alias("urban"),
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
        population = get_city_ppl(spark).select("city", "区县", "总人口", "常住人口", "城镇人口", "农村人口")
        area_code = get_area(spark).where(col("city").rlike("株洲市|邵阳市|岳阳市")) \
                                  .select("county", "sale_center_id")
        area_plt = population.join(area_code, col("county") == col("区县"))

        cols=["total_number","permanent_number","urban_number","country_number"]

        # 每个区域中心的人口
        sale_center_ppl = area_plt.groupBy("city", "sale_center_id") \
                                  .agg(f.sum("总人口").alias(cols[0]), f.sum("常住人口").alias(cols[1]),
                                         f.sum("城镇人口").alias(cols[2]), f.sum("农村人口").alias(cols[3]))

        sale_center_ppl.foreachPartition(lambda x:write_hbase1(x,cols,hbase))
    except Exception:
        tb.print_exc()







