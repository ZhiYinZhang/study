#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/6/21 11:21
from pyspark.sql.functions import col
from pyspark.sql import functions as f
from pyspark.sql.types import *
from pyspark.ml.fpm import FPGrowth
from pyspark.ml.feature import Word2Vec
import pandas as pd
from datetime import date, timedelta


def combineWithSyn(x, model):
    try:
        return x + "," + model.findSynonymsArray(x, 1)[0][0]
    except:
        return None

def get_client_similar_cigar(spark):

    # 注册UDF函数，选择三个月的数据分析
    strlen = spark.udf.register("strlen", lambda x: len(x))
    begin_time = (date.today() - timedelta(days=90)).strftime("%Y%m%d")

    lines = spark.sql(
        f"SELECT co_num, item_id FROM DB2_DB2INST1_CO_CO_LINE WHERE \
            born_date > {begin_time} AND qty_rsn > 0 AND qty_ord > 0"
    )
    # 构造训练集
    # 单次订单少于2个品规的，不进行训练
    items_df = lines.groupBy("co_num").agg(f.collect_list("item_id").alias("sentence")).select("sentence")
    items_df = items_df.filter(strlen("sentence") >= 2)

    # Word2Vec需要调整参数
    word2Vec = Word2Vec(vectorSize=30, seed=88, inputCol="sentence", outputCol="model")
    model = word2Vec.fit(items_df)

    # 构造测试集
    raw_data = lines.select(col("item_id").alias("items")).distinct()
    raw_data = raw_data.dropna()

    raw_data = list(map(lambda x: x['items'], raw_data.collect()))
    predict_df = pd.DataFrame(list(filter(lambda x: x is not None, \
        map(lambda x: combineWithSyn(x, model), raw_data))))

    # 生成Spark DataFrame, origin为要查找的烟，nearest为最相似的卷烟
    predict = spark.createDataFrame(predict_df, ["value"])
    predict = predict.withColumn("value", f.split("value", r","))
    predict = predict.withColumn("origin", f.element_at("value", 1))
    predict = predict.withColumn("nearest", f.element_at("value", 2))
    predict = predict.drop("value")

    return predict



if __name__=="__main__":
    pass
