#!/usr/bin/env python
# -*- coding:utf-8 -*-
import pandas as pd

import random
import numpy as np
import json
from hdfs import client
from rabbitmqDemo import rabbitProducer as rp
import os
# cli = client.InsecureClient(url="http://entrobus11:50070",user="zhangzy",root='/user/zhangzy')
#
# with cli.read('DPT/dataset/model01_train.parquet') as file:
#     df = pd.read_parquet(file,engine="fastparquet")
pd.set_option("display.max_columns",10)
df_pd = pd.read_csv('e://pythonProject/dataset/model06_train.csv')
# df_pd = df_pd.drop(columns=['Unnamed: 0'])


df_pd = df_pd.fillna(0)

df_pd.to_csv('e://pythonProject/dataset/model06_train1.csv',index=False)
# print(df_pd.columns)
# df_pd.to_csv('e://pythonProject/dataset/model06_train.csv',index=False)