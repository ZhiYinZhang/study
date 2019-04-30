#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/11 14:59
import pandas as pd
from pyspark.sql import SparkSession
import os
def get_new_file(dir):
    # 获取创建时间最新的文件
    files = os.listdir(dir)
    tuples = []
    for file in files:
        path = dir + file
        t = (path, os.path.getctime(path))
        tuples.append(t)

    tuples = sorted(tuples, key=lambda x: x[1], reverse=True)

    target_file = tuples[0][0]
    return target_file

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

path="E:\\test\\csv"
get_new_file(path)
spark=SparkSession.builder.appName("pandas_spark")\
                    .master("local[3]")\
                    .getOrCreate()


