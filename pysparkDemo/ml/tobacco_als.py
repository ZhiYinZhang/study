#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/6/13 10:19
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col
from pyspark.sql import functions as f
from pyspark.ml.feature import StringIndexer,IndexToString


def string2index(df,columns:list,param):

    for column in columns:
        column_new=column+"_int"
        print(f"string2index {column} to {column_new}")

        stringIndexer = StringIndexer(inputCol=column, outputCol=column_new)
        model = stringIndexer.fit(df)

        #生成labels
        param[column+"_labels"]=model.labels
        df = model.transform(df)\
                  .withColumn(column,col(column_new))\
                   .drop(column_new)
    return df
def index2string(df,columns:list,param):
    for column in columns:
        column_new=column+"_str"
        print(f"index2string {column} to {column_new}")

        labels=param[column+"_labels"]
        model=IndexToString(inputCol=column,outputCol=column_new,labels=labels)

        df=model.transform(df)\
                 .withColumn(column,col(column_new))\
                 .drop(column_new)
    return df
def recommendForAllUsers(df,userCol,itemCol,ratingCol):
    """

    :param df:  包含三列，userId:String itemId:String rating:float
    :param userCol:
    :param itemCol:
    :param ratingCol:
    """
    param = {}

    df_int = string2index(df, [userCol, itemCol],param)


    als = ALS(maxIter=5, regParam=0.01, userCol=userCol, itemCol=itemCol, ratingCol=ratingCol,
              coldStartStrategy="drop")
    model = als.fit(df_int)

    #获取item的数量
    item_num = df.dropDuplicates([itemCol]).count()
    #为所有user推荐所有item    userId,[[itemId1,rating1],[itemId2,rating2],......]
    userRecs = model.recommendForAllUsers(item_num)
    #将一行转多行 并将数组列每个元素作为一列
    rdd1 = userRecs.withColumn("recommendations", f.explode(col("recommendations"))) \
                   .rdd.map(lambda x: (x[0],x[1][0],x[1][1]))

    result_int = spark.createDataFrame(rdd1).toDF(userCol,itemCol,ratingCol)

    # print(param)
    result = index2string(result_int, [userCol, itemCol],param)

    return result

if __name__=="__main__":

    data_dir="E:/资料/project/烟草/株洲/株洲工程_ALS_卷烟相似度/株洲工程/als代码及数据集/als_1.csv"
    result_dir="e://test/als/result1"

    spark = SparkSession\
            .builder\
            .appName("ALSExample")\
            .getOrCreate()

    training = spark.read.csv(data_dir,header=True)\
                      .withColumn("ratings", col("ratings").cast("float"))

    result=recommendForAllUsers(training,"cust_id","item_id","ratings")
    result.write.csv(result_dir,header=True,mode="overwrite")


