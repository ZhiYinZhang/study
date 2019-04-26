#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# born_datetime:2019/2/27 17:33
from pyspark.sql import SparkSession,DataFrame,Row
from pyspark.sql.functions import lit,col,sum,min,max
import time

# spark=SparkSession.builder \
#       .appName("structStreaming") \
#       .master("local[2]") \
#       .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")
from pyspark.sql.functions import countDistinct
import json
def get_info(df:DataFrame):
    """
       统计每一列的空值占比
       数值列的 summary(min,max,25%,75%,stddev等)
       string列的value类别
    :param df:
    :return:
    """
    #------------获取每一列的空值占比---------------------
    nl = ["null", "NULL", "", "n", "N"]
    cols = df.columns
    total = df.count()
    #每列的空值占比
    null_ratio = {}
    for c in cols:
        count = df.where(df[c].isin(nl)).count()
        # print(c, (count / total) * 100)
        null_ratio[c] = (count / total) * 100
    null_ratio=json.dumps(null_ratio)

    #------------获取数值列的summary和string列的值的类别--------
    types = df.dtypes
    types_str = []
    types_int = []
    for tp in types:
        if tp[0] == "dt":
            pass
        elif tp[1] == "string":
            types_str.append(tp[0])
        else:
            types_int.append(tp[0])
    #count,mean,sttdev,min,25%,50%,75%,max
    summary = df.select(types_int).summary().toJSON().collect()
    temp = {}
    for sm in summary:
        sm = json.loads(sm)
        key = sm.pop("summary")
        temp[key] = sm
    summary=json.dumps(temp)

    #string列的值类别
    value_count = df.select([countDistinct(tp).alias(tp) for tp in types_str]).toJSON().collect()[0]
    #date列最大最小
    if "dt" in cols:
        dt=df.select(min("dt"),max("dt")).toJSON().collect()[0]


    return {"null_ratio":null_ratio,"summary":summary,"value_count":value_count,"dt":dt}
if __name__=="__main__":

            spark: SparkSession = SparkSession.builder\
                                            .config("spark.driver.extraClassPath=E:\mysql-connector-java-5.1.6.jar")\
                                            .config("spark.executor.extraClassPath=E:\mysql-connector-java-5.1.6.jar")\
                                            .appName("demo")\
                                            .master("local[3]")\
                                            .getOrCreate()

            sc = spark.sparkContext
            sc.setLogLevel("WARN")

            url="jdbc:mysql://localhost:3306/entrobus"
            # dbtable="qm_task_amount"
            # prop={"driver":"com.mysql.jdbc.Driver","user":"root","password":"123456"}
            df=spark.read.format("jdbc").options(url=url,driver="com.mysql.jdbc.Driver"
                                  ,dbtable="qm_task_amount",user="root",password="123456").load()
            df.show()
