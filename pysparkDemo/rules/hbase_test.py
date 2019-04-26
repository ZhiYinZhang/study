#-*- coding: utf-8 -*-
# @Time    : 2019/4/20 23:11
# @Author  : Z
# @Email   : S
# @File    : hbase_test.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import psutil
import time
import traceback as tb
try:
    1/0
except Exception as e:
    tb.print_exc()
print(1)
