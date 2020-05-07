#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/3/26 15:25
import pymysql
import traceback as tb
from pyspark.sql import SparkSession,functions as f
from pyspark.sql.functions import col
import time


host = "10.18.0.32"
user = "zzy"
password = "123456"
db = "test"
table="test1"

#第一种方法
def process_row(row):
    connect = pymysql.connect(host, user, password, db)
    cursor = connect.cursor()
    id = row["value"]
    col1 = row["col1"]
    col2 = row["col2"]
    col3 = row["col3"]
    try:
        sql = f"insert into test1(id,col1,col2,col3)values({id},'{col1}','{col2}','{col3}')"
        cursor.execute(sql)
        connect.commit()

    except:
        connect.rollback()

#第二种方法
class ForeachWriter:
    connect = None
    cursor = None
    def open(self, partition_id, epoch_id):
        # 获取数据库连接
        global connect, cursor
        connect = pymysql.connect(host, user, password, db)
        cursor = connect.cursor()
        return True

    def process(self, row):

        id = row["value"]
        col1 = row["col1"]
        col2 = row["col2"]
        col3 = row["col3"]
        try:
            # sql = f"insert into test1(id,col1,col2,col3)values({id},'{col1}','{col2}','{col3}')"
            # cursor.execute(sql)
            # connect.commit()

            # 查询是否存在
            sql = f"select * from {table} where id={id}"
            cursor.execute(sql)
            result = cursor.fetchone()
            if result:
                # 存在，更新
                sql = f"update {table} set col1='{col1}_new',col2='{col2}_new',col3='{col3}_new' where id={id}"
                cursor.execute(sql)
                connect.commit()
            else:
                # 不存在，插入
                sql = f"insert into {table}(id,col1,col2,col3)values({id},'{col1}','{col2}','{col3}')"
                cursor.execute(sql)
                connect.commit()
        except:
            connect.rollback()
            tb.print_exc()

    def close(self, error):
        connect.close()

if __name__=="__main__":
    spark=SparkSession.builder.appName("foreach sink").master("local[*]").getOrCreate()
    df = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", 1) \
        .load()

    result = df.withColumn("col1", f.concat_ws("_", f.lit("a"), col("value"))) \
        .withColumn("col2", f.concat_ws("_", f.lit("b"), col("value"))) \
        .withColumn("col3", f.concat_ws("_", f.lit("c"), col("value")))

    # query = result.writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .option("truncate", "flase") \
    #     .foreach(process_row) \
    #     .start()

    query = result.writeStream \
        .outputMode("append") \
        .option("truncate", "flase") \
        .foreach(ForeachWriter()) \
        .start()

    while True:
        progress=query.lastProgress
        if progress is not None:
            print(progress)
        time.sleep(1)