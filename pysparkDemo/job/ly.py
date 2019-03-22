#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/3/21 11:03
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import lit,col,sum,min,max
import time
from pyspark.sql.functions import countDistinct
import json
def get_info(df:DataFrame):
    """
       统计每一列的空值占比
       数值列的 summary(min,max,25%,75%,stddev等)
       string列的value类别
    :param df:
    :return:
    """
    #------------获取每一列的空值占比---------------------
    nl = ["null", "NULL", "", "n", "N"]
    cols = df.columns
    total = df.count()
    #每列的空值占比
    null_ratio = {}
    for c in cols:
        count = df.where(df[c].isin(nl)).count()
        # print(c, (count / total) * 100)
        null_ratio[c] = (count / total) * 100
    null_ratio=json.dumps(null_ratio)

    #------------获取数值列的summary和string列的值的类别--------
    types = df.dtypes
    types_str = []
    types_int = []
    for tp in types:
        if tp[0] == "dt":
            pass
        elif tp[1] == "string":
            types_str.append(tp[0])
        else:
            types_int.append(tp[0])
    #count,mean,sttdev,min,25%,50%,75%,max
    summary = df.select(types_int).summary().toJSON().collect()
    temp = {}
    for sm in summary:
        sm = json.loads(sm)
        key = sm.pop("summary")
        temp[key] = sm
    summary=json.dumps(temp)

    #string列的值类别
    value_count = df.select([countDistinct(tp).alias(tp) for tp in types_str]).toJSON().collect()[0]
    #date列最大最小
    if "dt" in cols:
        dt=df.select(min("dt"),max("dt")).toJSON().collect()[0]


    return {"null_ratio":null_ratio,"summary":summary,"value_count":value_count,"dt":dt}

if __name__=="__main__":
    spark = SparkSession.builder \
        .appName("structStreaming") \
        .master("local[2]") \
        .getOrCreate()

    df = spark.read.csv("e://test/test1.csv", header=True, inferSchema=True)

    df = df.withColumn("seq", df["seq"].cast("integer")) \
        .withColumn("longitude", df["longitude"].cast("decimal(18,8)")) \
        .withColumn("latitude", df["latitude"].cast("decimal(18,8)"))
    df.printSchema()
    result = get_info(df)
    print(result)
    spark.createDataFrame(data=[result]).show()