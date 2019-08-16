#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/6/21 11:21
from pyspark.sql.functions import col
from pyspark.sql import functions as f
from pyspark.ml.feature import Word2Vec
import pandas as pd
from datetime import date, timedelta
from rules.utils import element_at

def combineWithSyn(x, model):
    try:
        return model.findSynonymsArray(x, 4)
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
    word2Vec = Word2Vec(vectorSize=30, seed=88,minCount=1, inputCol="sentence", outputCol="model")
    model = word2Vec.fit(items_df)

    # 构造测试集
    raw_data = lines.select(col("item_id").alias("items")).distinct()
    raw_data = raw_data.dropna()
    raw_data = list(map(lambda x: x['items'], raw_data.collect()))

    predict_df = pd.DataFrame(raw_data, columns=["origin"])
    predict_df["near_list"] = predict_df["origin"].apply(lambda x: combineWithSyn(x, model))

    out = []
    for v in predict_df.values:
        df = pd.DataFrame(v[1], columns=["other", "distance"])
        df["origin"] = v[0]
        out.append(df.copy())
    df = pd.concat(out)
    # 生成对应的sparkDataFrame   烟 相似的烟 相似度
    df = spark.createDataFrame(df[["origin", "other", "distance"]], ["origin", "other", "distance"])

    return df

if __name__=="__main__":
    pass
