#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/4/17 15:27
from pyspark.sql import functions as f
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pysparkDemo.rules.write_hbase import write_hbase1
from pysparkDemo.rules.utils import grade_diff_udf,box_plots_filter_udf
from datetime import datetime as dt

spark = SparkSession.builder.enableHiveSupport().appName("retail warning").getOrCreate()
spark.sql("use aistrong")

hbase={"table":"TOBACCO.RETAIL_WARNING","families":["0"],"row":"cust_id"}
hbase["table"]="test_ma"
hbase["families"]=["info"]
hbase["row"]="cust_id"

#-----实际经营人与持证人不符
def is_match(x, y):
    if x != y:
        result = "1"
    return result


isMatch = card_pass = f.udf(is_match)
#co_cust 全量更新
co_cust = spark.sql("select cust_id,identity_card_id,order_tel from DB2_DB2INST1_CO_CUST "
                    "where dt=(select max(dt) from DB2_DB2INST1_CO_CUST) and status !=04").coalesce(5)
#co_debit_acc 全量更新
co_debit_acc = spark.sql("select cust_id,pass_id,acc from DB2_DB2INST1_CO_DEBIT_ACC "
                         "where dt=(select max(dt) from DB2_DB2INST1_CO_DEBIT_ACC) and status=1").coalesce(5)
colName="license_not_match"
co_cust.join(co_debit_acc, "cust_id") \
    .withColumn(colName, isMatch(col("identity_card_id"), col("pass_id"))) \
    .where(col(colName)=="1")\
    .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))


#-----每个身份证对应 多零售户id
card_filter=co_cust.where(col("identity_card_id").rlike("(\w{15,})"))
card_num=card_filter.groupBy("identity_card_id")\
                .agg(f.count("cust_id").alias("cust_num"))\
                .where(col("cust_num")>1)
colName="one_id_more_retail"
card_filter.join(card_num,"identity_card_id")\
       .withColumn(colName,f.lit("1"))\
        .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))

#-----每个电话对应 多零售户id
tel_filter=co_cust.where(col("order_tel").rlike("[^(NULL)]"))
cust_num=tel_filter.groupBy("order_tel")\
                .agg(f.count("cust_id").alias("cust_num"))\
                .where(col("cust_num")>1)
colName="one_tel_more_retail"
tel_filter.join(cust_num,"order_tel")\
       .withColumn(colName,f.lit("1"))\
        .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))

#-----一个银行账号对应多个零售户
cust_num=co_debit_acc.groupBy("acc")\
            .agg(f.count("cust_id").alias("cust_num"))\
            .where(col("cust_num")>1)
colName="one_bank_more_retail"
co_debit_acc.join(cust_num,"acc")\
            .withColumn(colName,f.lit("1"))\
            .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))




#-----零售户90天内档位变更数值在箱式图的上限或下限之外

co_cust=spark.sql("select cust_id,com_id from DB2_DB2INST1_CO_CUST "
                  "where dt=(select max(dt) from DB2_DB2INST1_CO_CUST) and status !=04").coalesce(5)
#档位变更表
crm_cust_log = spark.sql("select cust_id,change_frm,change_to,audit_date from DB2_DB2INST1_CRM_CUST_LOG where change_type='CO_CUST.CUST_SEG'") \
    .withColumn("audit_date", f.to_date("audit_date", "yyyyMMdd")) \
    .where(f.datediff(f.current_date(), col("audit_date")) <= 90)\
    .withColumn("change_frm",f.regexp_replace(col("change_frm"),"(zz)|(ZZ)","31"))\
    .withColumn("change_to",f.regexp_replace(col("change_to"),"(zz)|(ZZ)","31"))
#档位变更差
grade_diff_df=crm_cust_log.groupBy("cust_id")\
           .agg(grade_diff_udf(f.max("change_frm"), f.min("change_frm"), f.max("change_to"), f.min("change_to")).alias("grade_diff"))\
           .join(co_cust,"cust_id")
grade_diff_df.registerTempTable("grade_diff_df")

colName="grade_abs_three_month"
print(f"{str(dt.now())}   档位变更差异常的")
result=spark.sql("select com_id,percentile_approx(grade_diff,0.25) as percent_25,percentile_approx(grade_diff,0.75) as percent_75 from grade_diff_df group by com_id")\
        .join(grade_diff_df,"com_id")\
        .withColumn(colName,box_plots_filter_udf(col("grade_diff"),col("percent_25"),col("percent_75")))\
        .where(col(colName)==1)
print(f"{str(dt.now())}  档位变更差")
grade_diff_df.join(result,"cust_id")\
             .foreachPartition(lambda x:write_hbase1(x,["grade_diff"],hbase))

print(f"{str(dt.now())}   档位变更次数")
crm_cust_log.groupBy("cust_id").count()\
            .join(result,"cust_id")\
            .foreachPartition(lambda x:write_hbase1(x,["count"],hbase))

print(f"{str(dt.now())}   档位变更差")
colName="grade_before"
crm_cust_log.groupBy("cust_id") \
    .agg(f.min("audit_date").alias("audit_date")) \
    .join(crm_cust_log, ["cust_id", "audit_date"]) \
    .withColumnRenamed("change_frm", colName) \
    .foreachPartition(lambda x: write_hbase1(x, [colName],hbase))
