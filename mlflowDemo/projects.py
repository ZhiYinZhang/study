#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/11/11 10:52
import os
import warnings
import sys
import uuid
import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import ElasticNet
import mlflow
import mlflow.sklearn
import logging
logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)


def eval_metrics(actual, pred):
    rmse = np.sqrt(mean_squared_error(actual, pred))
    mae = mean_absolute_error(actual, pred)
    r2 = r2_score(actual, pred)
    return rmse, mae, r2



if __name__ == "__main__":
    # mlflow.set_tracking_uri("http://localhost:5001")
    # mlflow.set_tracking_uri("http://10.18.0.34:5000")
    # mlflow.set_experiment("elasticnet")
    warnings.filterwarnings("ignore")
    np.random.seed(40)

    data_path="E:\\github\\mlflow\\examples\\sklearn_elasticnet_wine\\wine-quality.csv"
    data=pd.read_csv(data_path)


    # Split the data into training and test sets. (0.75, 0.25) split.
    train, test = train_test_split(data)

    # The predicted column is "quality" which is a scalar from [3, 9]
    train_x = train.drop(["quality"], axis=1)
    test_x = test.drop(["quality"], axis=1)
    train_y = train[["quality"]]
    test_y = test[["quality"]]

    # alpha = float(sys.argv[1]) if len(sys.argv) > 1 else 0.5
    # l1_ratio = float(sys.argv[2]) if len(sys.argv) > 2 else 0.5
    alpha=0.5
    l1_ratio=0.5

    #mlflow.start_run()可以指定run_id，experiment_id，nested(嵌套运行)
    #nested在notebook中运行多个run_id或experiment_id时有用，不然一个notebook只能运行一个run_id
    run_id=str(uuid.uuid4())
    print(run_id)
    with mlflow.start_run(run_name=run_id,experiment_id="2"):
        lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, random_state=42)
        lr.fit(train_x, train_y)

        predicted_qualities = lr.predict(test_x)

        (rmse, mae, r2) = eval_metrics(test_y, predicted_qualities)

        print("Elasticnet model (alpha=%f, l1_ratio=%f):" % (alpha, l1_ratio))
        print("  RMSE: %s" % rmse)
        print("  MAE: %s" % mae)
        print("  R2: %s" % r2)

        mlflow.log_param("alpha", alpha)
        mlflow.log_param("l1_ratio", l1_ratio)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("r2", r2)
        mlflow.log_metric("mae", mae)
        mlflow.get_artifact_uri()
        mlflow.sklearn.log_model(lr,"model")