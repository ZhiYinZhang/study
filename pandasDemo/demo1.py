#!/usr/bin/env python
# -*- coding:utf-8 -*-
import pandas as pd

import random
import numpy as np
import json
from hdfs import client
from rabbitmqDemo import rabbitProducer as rp

cli = client.InsecureClient(url="http://entrobus11:50070",user="zhangzy",root='/user/zhangzy')

with cli.read('DPT/dataset/model01_train.parquet') as file:
    df = pd.read_parquet(file,engine="fastparquet")
