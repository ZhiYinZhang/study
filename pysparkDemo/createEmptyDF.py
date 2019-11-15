#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/11/7 8:56
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as f
from pyspark.sql.types import StructType,ShortType,IntegerType,LongType,\
    FloatType,DoubleType,StringType,BooleanType,DateType,TimestampType
"""
根据字段类型创建空的dataframe
"""
spark=SparkSession.builder.enableHiveSupport()\
                    .config("spark.sql.execution.arrow.enabled","true")\
                    .appName("cigar").getOrCreate()


type_map={"short":ShortType(),"int":IntegerType(),"long":LongType(),
          "float":FloatType(),"double":DoubleType(),"string":StringType(),
          "boolean":BooleanType(),"date":DateType(),"timestamp":TimestampType()
          }


fields={"id":"string","name":"string","age":"int"}

schema=StructType()
data=[[]]
for field in fields.keys():
    fieldType=type_map[fields[field]]
    schema=schema.add(field,fieldType)
    data[0].append(None)


df=spark.createDataFrame(data=data,schema=schema)


df.show()
df.printSchema()