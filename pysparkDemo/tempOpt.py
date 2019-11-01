#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/11 13:12
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col
from pyspark.sql.types import *
import traceback as tb
import datetime
from datetime import datetime as dt
import uuid
import json


spark=SparkSession.builder.appName("test").getOrCreate()

getUUID=f.udf(lambda :str(uuid.uuid4()).replace("-",""))
def custIsEqual(data):
    """
     判断 data里面的cust_id是否都是一样的，有一个不同就返回True
    data:  [{"co_num": "SY10113664", "cust_id": "430501171887"},{"co_num": "SY10113664", "cust_id": "430501171887"}]
    """
    #集合 不重复
    cust_ids=set()
    for i in data:
        cust_id=json.loads(i)["cust_id"]
        cust_ids.add(cust_id)
    if len(cust_ids)>1:
        return True
    else:
        return False
custIsEqualUdf=f.udf(custIsEqual,BooleanType())





def get_plm_item(spark):
    return spark.read.csv("e://test//tobacco_hive//plm_item.csv",header=True)

def get_co_co_line(spark):
    return spark.read.parquet("e://test//tobacco_hive//co_co_line.parquet")

def get_same_order_except():
    """
        近30天 属于同一营销部，且订购数量，金额，卷烟种类相同的订单，但不属于同一个零售户
        :return:
        """
    try:
        print(f"{str(dt.now())}    雷同订单")

        # def getItemName(items):
        #     itemNames = []
        #     for itemId in items:
        #         itemName = item_json[itemId]
        #         itemNames.append(itemName)
        #     return itemNames
        # getItemNameUdf = f.udf(getItemName, ArrayType(StringType()))

        # 获取 卷烟id和名称的映射  {"item_id":"item_name"}
        plm_item = get_plm_item(spark)
        items = plm_item.select("item_id", "item_name") \
            .collect()
        item_json = {}
        for row in items:
            item_json[row["item_id"]] = row["item_name"]


        # #营销部名称
        # organ = spark.sql("select * from DB2_DB2INST1_PUB_ORGAN where dt=(select max(dt) from DB2_DB2INST1_PUB_ORGAN)") \
        #     .select("organ_code", "organ_name") \
        #     .withColumnRenamed("organ_name", "sale_dept_name")

        #
        # diff = datetime.timedelta(days=7)
        # threshold = (dt.now() - diff).strftime("%Y-%m-%d")

        # -----开始统计

        # 获取邵阳的订单
        # co_co_line = spark.sql(f"select * from DB2_DB2INST1_CO_CO_LINE where dt>='{threshold}'") \
        #                   .where(col("com_id").isin(["011114305"]))

        co_co_line=get_co_co_line(spark)\
                 .where(col("com_id").isin(["011114305"]))

        # 每个订单 的总订单数 总价格  卷烟集合
        # 把items 进行排序 是为了 在group时，里面元素相同的能属于同一组
        single_order = co_co_line.where(col("qty_ord") > 0) \
            .groupBy("co_num") \
            .agg(f.sum("qty_ord").alias("qty_ord"), f.sum("amt").alias("amt"), f.collect_list("item_id").alias("items")) \
            .withColumn("items", f.array_sort(col("items")))

        # 订单去重
        dis_df = co_co_line.dropDuplicates(["co_num"]) \
                            .select("co_num", "cust_id", "sale_dept_id") \
                            .join(single_order, "co_num")

        json_udf = f.udf(
            lambda x, y: json.dumps({"co_num": x, "cust_id": y}, ensure_ascii=False))
        #组装结果co_num_cust 并过滤掉co_num_cust里面零售户是一样的
        co_num_cust = dis_df.withColumn("co_num_cust_json", json_udf(col("co_num"), col("cust_id"))) \
            .groupBy("sale_dept_id", "qty_ord", "amt", "items") \
            .agg(f.collect_list("co_num_cust_json").alias("co_num_cust")) \
            .withColumn("isEqual",custIsEqualUdf(col("co_num_cust")))
            # .where(custIsEqualUdf(col("co_num_cust")))

        co_num_cust.show(truncate=False)

        # result = co_num_cust.withColumn("item_names", getItemNameUdf(col("items"))) \
        #     .join(organ, col("sale_dept_id") == col("organ_code")) \
        #     .withColumn("alarm_date", f.date_format(f.current_date(), "yyyyMMdd")) \
        #     .withColumn("classify_level1_code", f.lit("YJFL023")) \
        #     .withColumn("classify_level2_code", f.lit("YJFL023003")) \
        #     .withColumn("warning_level_code", f.lit("B1")) \
        #     .withColumn("classify_id", f.concat_ws("_", "classify_level1_code", getUUID())) \
        #     .withColumnRenamed("qty_ord", "retail_30_order_num") \
        #     .withColumnRenamed("amt", "retail_30_order_price") \
        #     .withColumn("city", f.lit("邵阳市"))
        # cols = ["city","sale_dept_name", "retail_30_order_num", "retail_30_order_price", "item_names", "co_num_cust",
        #         "alarm_date", "classify_level1_code", "classify_level2_code", "warning_level_code"
        #         ]
        # result.foreachPartition(lambda x:write_hbase1(x,cols,hbase))
    except:
        tb.print_exc()


if __name__=="__main__":
    get_same_order_except()