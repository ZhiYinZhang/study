#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/9/26 11:40
#!/usr/bin/env python
# -*- coding:utf-8 -*-
from pyspark.sql import SparkSession,DataFrame,Row,Window
from pyspark import SparkContext
from pyspark.accumulators import *
from pyspark.sql import functions
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import time
spark=SparkSession.builder \
    .appName("vectorAssembler") \
    .master("local[2]") \
    .getOrCreate()
sc:SparkContext = spark.sparkContext
sc.setLocalProperty(key="spark.yarn.queue",value="badis")


# sc.addPyFile("/home/zhangzy/DPT/lib.zip")

df = spark.read\
     .option('inferSchema',"true")\
     .option('header','true')\
     .csv('e://pythonProject/dataset/model07')

def addIndex(index_name, row):
    global i
    d = dict({index_name: i}, **row.asDict())

    i+=1
    return Row(**d)
#方法一：遍历
# start1=time.time()
# i=0
# index_name="index"
# r=df.rdd.map(lambda row:addIndex(index_name,row))
# df3=spark.createDataFrame(r)
# end1=time.time()
# df3.show()
# df3.write.csv(path="e://pythonProject/dataset/model7_test",mode='overwrite',header=True)

# df3 = df3.select('index')
# df3.printSchema()

#方法二：使用函数  类似于hive sql中 row_number() over(partition by "" order by "")

# start2=time.time()
# w=Window.orderBy(df.columns[0])
# df4=df.withColumn("index",functions.row_number().over(w))
# end2=time.time()
# df4.show(truncate=False)






# print(end1-start1)
# print(end2-start2)