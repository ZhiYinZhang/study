#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/29 16:48
from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import col
from pyspark.sql import functions as f
from datetime import datetime as dt
from hbaseDemo.hbaseOpt import write_hbase1

import pandas as pd
def _to_pandas(rows):
    pd_df=pd.DataFrame(list(rows))
    return [pd_df]
def to_pandas(df,num_partitions=None):
    """

    :param df: spark DataFrame
    :param num_partitions: 设置spark DataFrame的partition数量 默认：None
    :return:
    """
    if num_partitions is not None:
        df=df.repartition(num_partitions)
    pd_dfs=df.rdd.mapPartitions(_to_pandas).collect()
    pd_df=pd.concat(pd_dfs)
    pd_df.columns=df.columns

    return pd_df

spark = SparkSession.builder \
            .appName("smartCity")\
            .master("local[5]")\
            .getOrCreate()


df=spark.range(50).withColumn("value",f.md5(f.lit("abcd")))


df2=df.cache()



