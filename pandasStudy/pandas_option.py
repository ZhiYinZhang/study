#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2018/12/12 16:57
import pandas as pd
import numpy as np
from pyspark.sql import Window,SparkSession
from pyspark import SparkContext
from pandas.core.groupby.groupby import DataFrameGroupBy
pd.set_option("display.colheader_justify","left")
pd.set_option("display.expand_frame_repr",False)
row = 5
col = 2
df = pd.DataFrame(data=np.random.randn(row,col),columns=range(col))
df1 = pd.DataFrame(data=[(1,1.0),(1,2.0),(2,3.0),(2,5.0),(1,10.0)],columns=("id","v"))


df2 = pd.DataFrame([(1,)+(df1.v.mean(),)])


spark = SparkSession.\
    builder\
    .appName("test")\
    .master("local[2]") \
    .config("spark.app.name","test1") \
    .config("spark.pyspark.eagerEval.enabled","true")\
    .getOrCreate()


sc:SparkContext = spark.sparkContext

print(sc.getConf().contains("spark.pyspark.eagerEval.enabled"))
print(sc.getConf().getAll())
print(spark.conf.get("spark.pyspark.eagerEval.enabled"))