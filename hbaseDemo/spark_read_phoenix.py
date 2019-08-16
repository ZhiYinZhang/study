#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/6/4 17:38
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as f
from pyspark.sql.functions import col
import json
#第一种:拷贝phoenix-4.14.2-HBase-1.3-client.jar到 {spark_home}/jars
#第二种:指定spark.driver.extraClassPath，spark.executor.extraClassPath
spark = SparkSession.builder.appName("spark hbase") \
    .master("local[*]") \
    .config("spark.driver.extraClassPath", "E://test/phoenix_dpd/*") \
    .config("spark.executor.extraClassPath", "e://test/phoenix_dpd/*") \
    .getOrCreate()
sc:SparkContext = spark.sparkContext


# zkUrl="10.72.59.91:2181"
zkUrl="10.18.0.34:2181"
table="v630_tobacco.ciga_picture"
df=spark.read.format("org.apache.phoenix.spark")\
        .option("table",table)\
        .option("zkUrl",zkUrl)\
        .load()

json_udf=f.udf(f=lambda x:json.dumps(x))
df.select("row","gauge_id","gauge_prop_like_ciga")\
     .where(col("gauge_prop_like_ciga").isNotNull())\
    .show(truncate=False)
     # .groupBy("row")\
     # .agg(f.collect_list(col("gauge_prop_like_ciga")).alias("gauge_prop_like_ciga"))\
     # .withColumn("gauge_prop_like_ciga",json_udf(col("gauge_prop_like_ciga")))\
     # .write \
     #  .format("org.apache.phoenix.spark") \
     #  .mode("overwrite") \
     #  .option("table",table) \
     #  .option("zkUrl",zkUrl ) \
     #  .save()






# df=spark.range(10)\
#           .withColumn("id",col("id").cast("string"))\
#           .withColumn("name",f.lit("tom111"))


# df.write \
#   .format("org.apache.phoenix.spark") \
#   .mode("overwrite") \
#   .option("table", "tobacco.area") \
#   .option("zkUrl",zkUrl ) \
#   .save()

# ph_df.show()
# ph_df.write.csv("e://test//retail",header=True,mode="overwrite")
# ph_df.show()
# ph_df.groupBy("SALE_CENTER_ID").count().show()

