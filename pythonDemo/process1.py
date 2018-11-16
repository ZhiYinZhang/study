#!/usr/bin/env python
# -*- coding:utf-8 -*-
from pyspark.sql import SparkSession,DataFrame,Row,Window,functions
from pyspark.ml.linalg import Vectors
from pyspark import SparkJobInfo
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import subprocess as sp
import time
spark = SparkSession.builder\
            .appName("test")\
            .master("local[2]")\
            .getOrCreate()
start = time.time()
df = spark.read\
     .option('inferSchema',"true")\
     .option('header','true')\
     .csv('e://pythonProject/dataset/model7_test')
df.show()
end = time.time()
print(end-start)
# df.write.csv(path='e://pythonProject//dataset/model07',header=True,mode='overwrite')
#5.8133323192596436
