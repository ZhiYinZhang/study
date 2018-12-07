#-*- coding: utf-8 -*-
# @Time    : 2018/6/27 22:52
# @Author  : Z
# @Email   : S
# @File    : demo1.py
from random import random, randint
from mlflow import log_metric, log_param, log_artifacts
import mlflow

mlflow.set_tracking_uri("http://10.18.0.12:5001")

import os
from random import random, randint

from mlflow import log_metric, log_param, log_artifacts

if __name__ == "__main__":
    print("Running mlflow_tracking.py")

    log_param("param1", randint(0, 100))

    log_metric("foo", random())
    log_metric("foo", random() + 1)
    log_metric("foo", random() + 2)

    # if not os.path.exists("outputs"):
    #    os.makedirs("outputs")
    # with open("outputs/test.txt", "w") as f:
    #    f.write("hello world!")

    # log_artifacts("outputs")

# with mlflow.start_run():
#             log_param("param1", randint(0, 100))
#             log_metric("foo", random())
#             log_metric("foo", random() + 1)
#             log_metric("foo", random() + 2)