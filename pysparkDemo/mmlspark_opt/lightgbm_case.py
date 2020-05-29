#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/5/28 16:09
"""
使用的是mmlspark下的notebooks/samples/Regression - Vowpal Wabbit vs. LightGBM vs. Linear Regressor.ipynb
lightGBM那部分代码
"""
import numpy as np
import pandas as pd
from sklearn.datasets import load_boston
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp") \
            .master("local[*]")\
            .config("spark.jars.packages", "com.microsoft.ml.spark:mmlspark_2.11:1.0.0-rc1") \
            .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven") \
            .getOrCreate()

from mmlspark.lightgbm import LightGBMRegressor


#下载数据集
# boston = load_boston()
#
# feature_cols = ['f' + str(i) for i in range(boston.data.shape[1])]
# header = ['target'] + feature_cols
#
# pd_df=pd.DataFrame(data=np.column_stack((boston.target, boston.data)), columns=header)
# df = spark.createDataFrame(pd_df).repartition(1)


# path="/home/zhangzy/dataset/boston.csv"
path="E:\\test\\mmlspark"
# pd_df.to_csv(path,index=False)
df=spark.read.csv(path,header=True,inferSchema=True)
feature_cols=df.columns[2:]


train_data, test_data = df.randomSplit([0.75, 0.25], seed=42)
train_data.cache()
test_data.cache()



featurizer = VectorAssembler(
    inputCols=feature_cols,
    outputCol='features'
)
lr_train_data = featurizer.transform(train_data)['target', 'features']
lr_test_data = featurizer.transform(test_data)['target', 'features']



lgr = LightGBMRegressor(
    objective='quantile',
    alpha=0.2,
    learningRate=0.3,
    numLeaves=31,
    labelCol='target',
    numIterations=100,
)


repartitioned_data = lr_train_data.repartition(1).cache()


lg_model = lgr.fit(repartitioned_data)
lg_predictions = lg_model.transform(lr_test_data)

lg_predictions.show()