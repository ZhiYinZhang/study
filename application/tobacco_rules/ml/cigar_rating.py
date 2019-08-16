#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/6/13 14:27
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col
from pyspark.sql import functions as f
from pyspark.ml.feature import StringIndexer,IndexToString


def string2index(df,columns:list,param):
    """

    :param df:
    :param columns: 需要stringIndexer的列
    :param param:传进来是空字典 用来存放stringInxer列对应的原始值：{colName1_lebels:['a','b'...],colName2_labels:['e','d','f']}
    :return:
    """
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
def recommendForAllUsers(spark,df,userCol,itemCol,ratingCol):
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
    #为每个user推荐所有item    userId,[[itemId1,rating1],[itemId2,rating2],......]
    userRecs = model.recommendForAllUsers(item_num)
    #将一行转多行 并将数组列每个元素作为一列
    rdd1 = userRecs.withColumn("recommendations", f.explode(col("recommendations"))) \
                   .rdd.map(lambda x: (x[0],x[1][0],x[1][1]))

    result_int = spark.createDataFrame(rdd1).toDF(userCol,itemCol,ratingCol)

    # print(param)
    result = index2string(result_int, [userCol, itemCol],param)

    return result



if __name__=="__main__":
    from pyspark.sql import SparkSession

    spark=SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    df=spark.read.csv("E:/test/als/sample_movielens_data.csv",header=True)\
                  .withColumn("rating",col("rating").cast("float"))\
                  .withColumn("id",f.lit(10))
    df.where(col("userId")=="0").show(100)

    df1=recommendForAllUsers(spark,df,"userId","movieId","rating")
    df1.show(100)
    # df1.repartition(1).write.csv("e:/test/als/result1",header=True,mode="overwrite")