#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/9/26 11:40
#!/usr/bin/env python
# -*- coding:utf-8 -*-
from pyspark.sql import SparkSession,DataFrame,Row,Window
from pyspark.sql import functions
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import time

spark=SparkSession.builder \
    .appName("vectorAssembler") \
    .master("local[2]") \
    .getOrCreate()

# index=pd.date_range("2018/09/01",periods=100000)

pd_df=pd.DataFrame(data=np.random.randn(100,4),columns=['a','b','c','d'])
l=[]
for i in range(100):
    l.append("abc"[np.random.randint(0,3)])
pd_df["e"] =l

df=spark.createDataFrame(data=pd_df)


def addIndex(index_name, row):
    global i
    d = dict({index_name: i}, **row.asDict())
    i = i + 1
    return Row(**d)
#方法一：遍历
# start1=time.time()
# i=0
# index_name="id"
# r=df.rdd.map(lambda row:addIndex(index_name,row))
# df3=spark.createDataFrame(r)
# end1=time.time()
# df3.show()

#方法二：使用函数  类似于hive sql中 row_number() over(partition by "" order by "")

start2=time.time()
w=Window.orderBy(df.columns[0])
df4=df.withColumn("index",functions.row_number().over(w))
end2=time.time()
df4.show(100)



# print(end1-start1)
print(end2-start2)