#-*- coding: utf-8 -*-
# @Time    : 2018/10/12 22:43
# @Author  : Z
# @Email   : S
# @File    : returnDataframe.py
from hdfsDemo.hdfsClient import *
import pandas as pd
from pyspark.sql import SparkSession
import numpy as np

def get_spark():
    spark = SparkSession.builder\
                .appName("get_spark")\
                .master("local[1]")\
                .getOrCreate()
    return spark

if __name__=="__main__""":
    # cli = get_hdfs_client()
    spark = get_spark()
    # with cli.read('DPT/dataset/model03_train.csv') as file:
    #             df_pd = pd.read_csv(file)
    # print(df_pd.dtypes)

    df_pd = pd.DataFrame(data=np.random.randn(10,3),columns=['a','b','c'])
    df = spark.createDataFrame(df_pd)
    # df.write.csv(path="e://javacode//test.csv",header=True,mode="overwrite")
    df = df.limit(1)
    df.show()
