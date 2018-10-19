#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/9/29 10:24
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.types import StructType


def  readFile():
    spark=SparkSession.builder \
                .appName("columnInfo") \
                .master("local[2]") \
                .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    df_in:DataFrame=spark.read \
         .format("json") \
         .option("path","e:/pythonProject/test.json") \
         .option("inferSchema","true") \
         .load()
    path="e:/pythonProject/GBDT_model_2_Output_1"
    print(df_in.dtypes)
    pd_df=df_in.toPandas()
    pd_df.to_json()
    # pd_df.to_json(path_or_buf=path,orient="records",lines=True)
    # df_in.write.json("e:/pythonProject/GBDT_model_2_Output_1")


import os,sys
import pandas as pd
def file_type():
    path="e:/pythonProject/rate.json"
    df=pd.read_json(path_or_buf=path)

if __name__=="__main__":
    file_type()
    # readFile()