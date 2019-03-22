#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# born_datetime:2019/2/27 17:33
from pyspark.sql import SparkSession,DataFrame,Row
from pyspark.sql.functions import lit,col,sum,min,max
import time

spark=SparkSession.builder \
      .appName("structStreaming") \
      .master("local[2]") \
      .getOrCreate()
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
    # get_lately().show()
    tables=["DB2_DB2INST1_SGP_CUSTTYPE_ITEM_LIMIT",
    "DB2_DB2INST1_SGP_CUSTTYPE_ITEM_SPW",
    "DB2_DB2INST1_SGP_CUSTTYPE_ITEMTYPE_LIMIT",
    "DB2_DB2INST1_SGP_CUT_STOP_SCHEME",
    "DB2_DB2INST1_SGP_CUT_STOP_SCHEME_LINE",
    "DB2_DB2INST1_SGP_ITEM_SPW",
    "DB2_DB2INST1_SGP_WEEK",
    "DB2_ZHRYZM_T_I_USER",
    "DB2_ZHRYZM_T_I_DEPT",
    "DB2_ZHRYZM_T_LIC_RLIC_INFO",
    "DB2_ZHRYZM_T_I_CUSTOMER_MARKET",
    "DB2_ZHRYZM_T_I_CODE_DIRECTORY",
    "DB2_ZHRYZM_T_I_GRID_LEVEL_THREE",
    "DB2_ZHRYZM_T_I_GRID_LEVEL_FOUR",
    "DB2_ZHRYZM_T_I_GRID_LEVEL_FIVE",
    "DB2_ZHRYZM_T_L_RLIC_APPLY_MAIN",
    "DB2_ZHRYZM_T_BSC_CODE_TYPE",
    "DB2_ZHRYZM_T_BSC_CODE_BASE",
    "DB2_ZRHYZM_T_C_CASEINFO",
    "DB2_ZRHYZM_T_C_PARTYINFO",
    "DB2_ZRHYZM_T_C_PARTYINFO_EX",
    "DB2_ZRHYZM_T_C_CASE_GOODS_DTL",
    "DB2_ZRHYZM_T_C_CAR",
    "DB2_ZRHYZM_T_I_DAY_PLAN",
    "DB2_ZRHYZM_T_I_DAY_PLAN_EXECUTOR",
    "DB2_ZRHYZM_T_I_CHECK_CLIENT_LIST",
    "DB2_ZRHYZM_T_I_CLIENT_SIGN",
    "DB2_ZRHYZM_T_I_CHECK_RESULT",
    "DB2_ZRHYZM_T_C_TRANS_RESULT",
    "DB2_ZRHYZM_T_C_CRIMINAL",
    "DB2_ZRHYZM_T_C_CRIMINAL_DTL",
    "DB2_ZRHYZM_T_TM_EMPLOYEE_BASE_INFO",
    "DB2_ZRHYZM_T_TM_MONOPOLY_STD_DEPT",
    "DB2_ZRHYZM_T_TM_MONOPOLY_STD_JOB",
    "DB2_ZRHYZM_STMB_BLACKLIST"]

    df=spark.read.csv("e://test/test1.csv",header=True,inferSchema=True)

    df=df.withColumn("seq",df["seq"].cast("integer"))\
        .withColumn("longitude",df["longitude"].cast("decimal(18,8)"))\
        .withColumn("latitude",df["latitude"].cast("decimal(18,8)"))
    df.printSchema()
    result=get_info(df)
    print(result)
    spark.createDataFrame(data=[result]).show()