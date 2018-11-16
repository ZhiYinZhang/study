#!/usr/bin/env python
# -*- coding:utf-8 -*-
import pandas as pd
from pandas import DataFrame
import random
import numpy as np
import json
from hdfs import client
from rabbitmqDemo import rabbitProducer as rp
import os
cli = client.InsecureClient(url="http://entrobus11:50070",user="zhangzy",root='/user/zhangzy')

#读取hdfs json文件
with cli.read('test3.json') as reader:
    # df:DataFrame = pd.read_json(reader)
    d = json.load(reader)
print(d)
# df['timestamp']= df['timestamp'].astype(str)
# print(df[:])

#读取本地文件
# with open(file="e://pythonProject//DPT//files//model01.json",mode='r') as file:
#     d = json.load(fp=file)
#写hdfs
# with cli.write(hdfs_path='test3.json',overwrite=True,encoding='utf-8') as writer:
#     json.dump(d,writer)

# with cli.write('test3.json',overwrite=True,encoding='utf-8') as writer:
#     json.dump(df.to_dict(orient='record'),writer)

# with cli.read('test3.json',)