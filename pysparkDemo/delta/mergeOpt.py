#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/5/29 17:33
from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from delta.tables import DeltaTable


def merge(spark, update, tableName, cols, key):
    """
    将DataFrame和delta表进行merge操作，insert操作要求DataFrame必须包含delta表所有的列
    当我们使用merge操作更新/插入delta表其中几列时，指定在DataFrame中不存在的列的值为null。

    注：DataFrame中要写入delta表的列要和delta表一样
    :param spark,SparkSession实例
    :param update,spark DataFrame
    :param tableName,要更新的delta表
    """
    # 如果没有dt列，创建当前日期的dt列
    if "dt" not in cols:
        update = update.withColumn("dt", f.current_date())
        cols.append("dt")

    # 1.构建merge条件
    mergeExpr = f"origin.{key}=update.{key}"
    print(f"merge expression:{mergeExpr}")

    # 2.构建更新表达式
    updateExpr = {}
    for c in cols:
        updateExpr[c] = f"update.{c}"

    print(f"update expression:{updateExpr}")

    origin = DeltaTable.forPath(spark, tableName)
    origin_cols = origin.toDF().columns

    # 3.构建插入表达式
    insertExpr = {}
    for origin_col in origin_cols:
        if origin_col in cols:
            insertExpr[origin_col] = f"update.{origin_col}"
        else:
            # 不存在,插入null值(不是字符串)
            insertExpr[origin_col] = "null"

    print(f"insert expression:{insertExpr}")

    # for origin_col in origin_cols:
    #     if origin_col not in cols:
    #         update=update.withColumn(origin_col,f.lit(None))

    origin.alias("origin") \
        .merge(update.alias("update"), mergeExpr) \
        .whenMatchedUpdate(set=updateExpr) \
        .whenNotMatchedInsert(values=insertExpr) \
        .execute()

if __name__=="__main__":
    deltaTable = "/user/delta/test"

    spark = SparkSession.builder.appName("delta").master("local[2]").getOrCreate()

    #创建delta表
    df = spark.createDataFrame(data=[[None for i in range(8)]],
                               schema="id long,c0 int,c1 long,c2 float,c3 double,c4 string,c5 date,c6 timestamp") \
        .withColumn("dt", f.current_date())
    df.limit(0).write.partitionBy("dt").format("delta").mode("append").save(deltaTable)

    #插入数据
    update = spark.range(0, 10)\
                  .withColumn("dt", f.current_date()) \
                   .withColumn("c1", f.lit(0).cast("long"))
    update.write.partitionBy("dt").format("delta").mode("append").save(deltaTable)


    update = spark.range(5, 15) \
        .withColumn("dt", f.current_date()) \
        .withColumn("c1", f.lit(1).cast("long"))


    merge(spark,update,deltaTable,["id","dt","c1"],"id")