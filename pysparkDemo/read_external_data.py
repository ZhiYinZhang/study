#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/6/10 10:24

from pyspark.sql.functions import col
cities = ["011114305", "011114306", "011114302"]
zkUrl="10.72.59.89:2181"
def get_coordinate(spark):
    coordinate = spark.read.format("org.apache.phoenix.spark") \
        .option("table", "COORDINATE") \
        .option("zkUrl",zkUrl ) \
        .load() \
        .withColumn("lng", col("LONGITUDE").cast("float")) \
        .withColumn("lat", col("LATITUDE").cast("float"))

    columns=coordinate.columns
    for column in columns:
        coordinate=coordinate.withColumnRenamed(column,column.lower())

    return coordinate


def get_area(spark):
    area_code=spark.read.format("org.apache.phoenix.spark") \
        .option("table", "CODE") \
        .option("zkUrl", zkUrl) \
        .load() \
        .where(col("COM_ID").isin(cities))

    columns = area_code.columns
    for column in columns:
        area_code = area_code.withColumnRenamed(column, column.lower())

    return area_code



from pyspark.sql import SparkSession
from pyspark.sql.functions import col
if __name__=="__main__":
    path="E:\\test\\retail"
    spark = SparkSession.builder.appName("spark phoenix") \
        .master("local[2]") \
        .getOrCreate()

    df=spark.read.csv(path,header=True)
    print(df.count())

    df1=df.where((col("STATUS")!='04') & (col("CITY")=="株洲市"))

    df1.write.csv("e://test//retail1",header=True,mode="overwrite")