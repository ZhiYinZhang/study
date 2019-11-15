#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/11 13:12
import os
import mlflow
from mlflow import projects
if __name__=="__main__":
    path = "e:\\github\\mlflow\\examples\\sklearn_elasticnet_wine"
    relative_path="../../../github//mlflow//examples//sklearn_elasticnet_wine"
    projects.run(uri=path,
                 parameters={"alpha":0.5},use_conda=False,
                 experiment_id="2")
