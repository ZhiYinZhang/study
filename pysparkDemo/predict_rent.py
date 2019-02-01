#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/29 16:47
from pyspark.sql.functions import udf
import pandas as pd
from pyspark.sql import SparkSession
import re
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

def read_df_from_pandas(filepath):
    ##
    # filepath: 文件路径
    ##
    spark = SparkSession.builder.appName('read_csv').getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    df = pd.read_csv(filepath)
    df = spark.createDataFrame(df)
    df = df.repartition(1)
    return df

def clean_rent(string):
    pat = re.compile(r'\d+')
    return int(re.findall(pat,string)[0])

def preprocess_rent(df, f=clean_rent):
    ##
    # df: 待处理dataframe
    # f: 处理函数
    ##
    udf_clean_rent = udf(f, 'float')
    df = df.withColumn('rent', udf_clean_rent(df['rent']))
    return df

def onehot_rent_type(x):
    if x == '整租':
        x = 1
    else:
        x = 0
    return x

def preprocess_rent_type(df, f=onehot_rent_type):
    ##
    # df: 待处理dataframe
    # f: 处理函数
    ##
    udf_onehot_rent_type = udf(f, 'int')
    df = df.withColumn('rent_type', udf_onehot_rent_type(df['rent_type']))
    return df

def onehot_decorate(x):
    if x == '豪华装修' or x == '精装修':
        return 1
    else:
        return 0

def preprocess_decorate(df, f=onehot_decorate):
    ##
    # df: 待处理dataframe
    # f: 处理函数
    ##
    udf_onehot_decorate = udf(f, 'int')
    df = df.withColumn('decorate', udf_onehot_decorate(df['decorate']))
    return df

def clean_area(string):
    pat = re.compile(r'\d+')
    return int(re.findall(pat,string)[0])

def preprocess_area(df,f=clean_area):
    ##
    # df: 待处理dataframe
    # f: 处理函数
    ##
    udf_clean_area = udf(f, 'float')
    df = df.withColumn('area', udf_clean_area(df['area']))
    return df

def build_linear_regression(df):
    ##
    # df: 训练集
    ##
    vectorAssembler = VectorAssembler(inputCols=['rent_type','decorate','area'], outputCol='feature')
    train = vectorAssembler.transform(df)
    train = train.select(['feature','rent'])
    lr = LinearRegression(featuresCol='feature', labelCol='rent', maxIter=10)
    lr_model = lr.fit(train)
    return lr_model

def round_num(x, num=0):
    return round(x, num)


def get_rent_prediction(df, model, f_round):
    ##
    # df: 测试集
    ##
    udf_round_num = udf(f_round, 'float')
    pred = model.transform(df)
    pred = pred.withColumn('round', udf_round_num(pred['prediction']))
    return pred
