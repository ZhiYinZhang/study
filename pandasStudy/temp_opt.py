#-*- coding: utf-8 -*-
# @Time    : 2019/3/9 14:37
# @Author  : Z
# @Email   : S
# @File    : temp_opt.py
import pandas as pd
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pysparkDemo.lng_lat import get_spark

import statsmodels.tsa.stattools as ts

data=[68.0, 65.0, 63.0, 60.0, 68.0, 68.0, 77.0, 74.0, 74.0, 60.0, 56.0, 50.0, 40.0, 56.0, 60.0, 53.0, 66.0, 76.0, 85.0, 84.0, 83.0, 63.0, 58.0, 55.0, 53.0, 61.0, 58.0, 64.0, 67.0, 45.0, 62.0, 56.0, 101.0, 93.0, 120.0, 108.0, 79.0, 52.0, 61.0, 68.0, 65.0, 58.0, 61.0, 59.0, 52.0, 44.0, 50.0, 44.0, 50.0, 50.0]

data=[68.0, 65.0]
result = ts.adfuller(data,maxlag=1)

print(result[0])