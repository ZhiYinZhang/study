#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/10/25 11:45
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.column import Column
from pyspark.ml.classification import *
from pyspark.ml.feature import *

def typeTransform(df:DataFrame,col,to_type):
         df= df.withColumn(col,df[col].cast(to_type))
         return df
spark = SparkSession.builder\
                    .appName("model06")\
                    .master("local[2]")\
                    .getOrCreate()
df_train = spark.read\
     .format('csv')\
     .option('inferSchema','true')\
     .option('header','true')\
     .load('e://pythonProject/dataset/model06_train.csv')
df_train.printSchema()


df_train = typeTransform(df_train,'2_total_fee','double')
df_train = typeTransform(df_train,'3_total_fee','double')
df_train = typeTransform(df_train,'age','int')
df_train = typeTransform(df_train,'gender','int')

# df_test = spark.read\
#                .format('csv')\
#                .option('inferSchema','true')\
#                .option('header','true')\
#                .option('nanValue','//N')\
#                .load('e://pythonProject/dataset/model06_test.csv')
#缺失值填充
# for col,tp in df_train.dtypes:
#     if tp=='string':
#         print(tp)
#         df_train = df_train.na.fill({col:'0'})
#     else:
#         print(col,tp)
#         df_train = df_train.na.fill({col:0})
df_train = df_train.na.fill({'2_total_fee':0})
df_train = df_train.na.fill({'3_total_fee':0})
df_train = df_train.na.fill({'gender':0})
df_train = df_train.na.fill({'age':0})

df_train.printSchema()

# #string->index
for col,tp in df_train.dtypes:
    if tp =='string' or col=='current_service':
        string_indexer = StringIndexer(inputCol=col,outputCol=col+'_index')
        model = string_indexer.fit(df_train)
        df_train = model.transform(df_train)

df_train.printSchema()
# #vector
vector_col=[]
for col,tp in df_train.dtypes:
    if col!='current_service' and tp!='string' and col!='current_service_index' and col!='user_id' and col!='user_id_index':
        vector_col.append(col)
# print('vector',vector_col)
# for col in vector_col:
#     print(col,df_train.select(col).distinct().count())
vc_assembler = VectorAssembler(inputCols=vector_col,outputCol='features')
df_train = vc_assembler.transform(df_train)

# df_train = df_train.repartition(1)
# df_train.write.parquet('e://pythonProject//dataset/model06',mode='overwrite')
# #sample
df_validate = df_train.sample(0.3)
df_train01 = df_train.sample(0.7)

# #train
rf = RandomForestClassifier(numTrees=10,maxDepth=8,labelCol='current_service_index',seed=88,maxBins=428615)
model = rf.fit(df_train01)