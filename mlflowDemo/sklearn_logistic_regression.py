#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/11/11 11:56
import numpy as np
from sklearn.linear_model import LogisticRegression

import mlflow
from mlflow import sklearn


if __name__ == "__main__":
    mlflow.set_tracking_uri("http://localhost:5001")
    # mlflow.create_experiment("sklearn logistic regression")
    mlflow.set_experiment("sklearn logistic regression")
    with mlflow.start_run() as active_run:
        print(mlflow.active_run().info)


        X = np.array([-2, -1, 0, 1, 2, 1]).reshape(-1, 1)
        y = np.array([0, 0, 1, 1, 1, 0])
        lr = LogisticRegression()
        lr.fit(X, y)
        score = lr.score(X, y)
        print("Score: %s" % score)
        mlflow.log_metric("score", score)

        # sklearn.log_model(lr, "model")



        mlflow.sklearn.log_model(lr,"model2")
        # print("Model saved in run %s" % mlflow.active_run().info.run_uuid)