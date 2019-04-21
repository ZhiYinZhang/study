#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/4/17 15:27
from pyspark.sql import functions as f
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pysparkDemo.rules.write_hbase import write_hbase1
from datetime import datetime as dt
spark = SparkSession.builder.enableHiveSupport().appName("retail warning").getOrCreate()
spark.sql("use aistrong")

hbase={"table":"TOBACCO.RETAIL_WARNING","families":["0"],"row":"cust_id"}
hbase["table"]="test_ma"
hbase["families"]=["info"]
hbase["row"]="cust_id"

# 实际经营人与持证人不符
def is_match(x, y):
    if x != y:
        result = "1"
    return result


isMatch = card_pass = f.udf(is_match)
#co_cust 全量更新
co_cust = spark.sql("select cust_id,identity_card_id,order_tel from DB2_DB2INST1_CO_CUST "
                    "where dt=(select max(dt) from DB2_DB2INST1_CO_CUST) and status !=04").coalesce(5)
#co_debit_acc 全量更新
co_debit_acc = spark.sql("select cust_id,pass_id,bank_id from DB2_DB2INST1_CO_DEBIT_ACC "
                         "where dt=(select max(dt) from DB2_DB2INST1_CO_DEBIT_ACC) and status=1").coalesce(5)
colName="license_not_match"
co_cust.join(co_debit_acc, "cust_id") \
    .withColumn(colName, isMatch(col("identity_card_id"), col("pass_id"))) \
    .where(col(colName)=="1")\
    .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))


#每个身份证对应 多零售户id
card_filter=co_cust.where(col("identity_card_id").rlike("(\w{15,})"))
card_num=card_filter.groupBy("identity_card_id")\
                .agg(f.count("cust_id").alias("cust_num"))\
                .where(col("cust_num")>1)
colName="one_id_more_retail"
card_filter.join(card_num,"identity_card_id")\
       .withColumn(colName,f.lit("1"))\
        .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))

#每个电话对应 多零售户id
tel_filter=co_cust.where(col("order_tel").rlike("[^(NULL)]"))
cust_num=tel_filter.groupBy("order_tel")\
                .agg(f.count("cust_id").alias("cust_num"))\
                .where(col("cust_num")>1)
colName="one_tel_more_retail"
tel_filter.join(cust_num,"order_tel")\
       .withColumn(colName,f.lit("1"))\
        .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))

#一个银行账号对应多个零售户
cust_num=co_debit_acc.groupBy("bank_id")\
            .agg(f.count("cust_id").alias("cust_num"))\
            .where(col("cust_num")>1)
colName="one_bank_more_retail"
co_debit_acc.join(cust_num,"bank_id")\
            .withColumn(colName,f.lit("1"))\
            .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))