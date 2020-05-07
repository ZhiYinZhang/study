#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/11/14 18:25
# import mlflow
# import mlflow.sklearn
# mlflow.set_experiment("sklearn logistic regression")
# model=mlflow.sklearn.load_model("runs:/abe4a3f19c6a45de9fe70a77fee81023/model")
# mlflow.set_experiment()
# import datetime
# datetime.timedelta()


import pandas as pd
import numpy as np

df=pd.DataFrame(np.random.randint(1,10,100),columns=["a"])

df.to_json(orient="")