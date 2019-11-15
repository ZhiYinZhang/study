#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/11/14 17:56
from mlflow import pyfunc
import mlflow
"""
将pandas的值加上5
"""
class addN(mlflow.pyfunc.PythonModel):
    def __init__(self,n):
        self.n=n
    def predict(self,context,model_input):

        return model_input.apply(lambda column:column+self.n)

#保存
model_path="add_n_model"
add5_model=addN(n=5)
pyfunc.save_model(path=model_path,python_model=add5_model)

#加载
loaded_model=pyfunc.load_model(model_path)


#执行
import pandas as pd
import numpy as np
model_input=pd.DataFrame(np.random.randint(1,10,5))


model_output=loaded_model.predict(model_input)

print(model_input)
print(model_output)