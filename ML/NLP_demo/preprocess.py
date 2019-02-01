#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/24 10:12
import pandas as pd
import numpy as np

import lightgbm as lgb

from sklearn.model_selection import  KFold
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import make_scorer

from pyspark.ml.tuning import CrossValidator
from mmlspark import LightGBMRegressor

# today_str = datetime.date.today().strftime("%Y%m%d")
today_str = "20190126"


def mae_metric(y_true, predict):
    mae = mean_absolute_error(y_true, predict)
    return 1/(1+mae)


my_score = make_scorer(mae_metric, greater_is_better=True)
train_path = "E:/lgb/train_" + today_str + ".csv"
test_path = "E:/lgb/test_" + today_str + ".csv"
train_df = pd.read_csv(train_path)
test_df = pd.read_csv(test_path)



X_train, y_train = train_df.drop(['user_id', 'target'], axis=1), train_df['target']
X_test = test_df.drop(['user_id'], axis=1).values
X_train = X_train.values
y_train = y_train.values





lgb = LightGBMRegressor(objective="quantile",alpha=0.2,learningRate=0.01,numLeaves=31)



cv = CrossValidator(estimator=lgb,numFolds=5)


