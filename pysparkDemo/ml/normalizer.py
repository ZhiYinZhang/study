#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/3/1 14:28
from mmlspark import LightGBMRegressor
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession
# spark=SparkSession.builder \
#       .appName("normalizer") \
#       .master("local[2]") \
#       .getOrCreate()

svec=Vectors.sparse(4,{1:4.0,3:3.0})
dvec=Vectors.dense([3.0,-4.0])
print(svec)
print(dvec)