#-*- coding: utf-8 -*-
# @Time    : 2018/10/20 18:30
# @Author  : Z
# @Email   : S
# @File    : demo1.py
from hdfsDemo.hdfsClient import *
import re
import pandas as pd

cli = get_hdfs_client()
path = "DPT/temp/21C5BCE840B3AE9170C5A69EF62B40E9/E3204CA4A11B1F72E3768CC314190150/model02_train.csv"

if cli.status(hdfs_path=path,strict=False):
       for i in cli.list(path):
            if re.findall(r".*(.csv)$",i):
                with cli.read(hdfs_path=f"{path}/{i}") as file:
                    df_pd = pd.read_csv(file)


print(df_pd)