#-*- coding: utf-8 -*-
# @Time    : 2019/3/9 19:21
# @Author  : Z
# @Email   : S
# @File    : excel_csv.py

import pandas as pd
import os
from pyspark.sql import SparkSession

def excel_csv(path):
     par_dir=os.path.dirname(path)
     file_name=os.path.basename(path).split(".")[0]

     df_pd = pd.read_excel(path)
     df_pd.to_csv(f"{par_dir}/{file_name}.csv",index=False)


base_dir="E:/dataset/yancao/株洲-coordinate-v2"


data1="e:/dataset/yancao/株洲-烟草零售.xls"

for d in os.listdir(base_dir):
    print(d)
    files=os.listdir(os.path.join(base_dir,d))
    print(files)

    for file in files:
        path=os.path.join(base_dir,d,file)
        print(path)
        excel_csv(path)
    print("------------------------------------------------------------")
