#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/10/19 10:01
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler,StringIndexer
import time

def preProcess(df_in,args):

    col = df_in.columns

    print(col)
    if len(args["labelColName"]) == 0 or args["labelColName"][0] not in col:
        if 'label' in col:
            args["labelColName"] = "label"
            col.remove('label')
        else:
            lab = col[-1]
            args['labelColName'] = lab
            col.remove(lab)
        args['featureColNames'] = col
    else:
        # 把labelColName list->string
        args['labelColName'] = args['labelColName'][0]
        if args['featureColNames'] not in col:  # 永远不会进来
            col.remove(args['labelColName'])
            args["featureColNames"] = col

    if "features" not in col:
        # index the string label    column_type: [(col1,col1Type),(col2,col2Type)]
        for column_type in df_in.dtypes:
            if column_type[0] not in args["labelColName"]:
                tp = column_type[1]
                if tp == 'string':

                    df_in = StringIndexer(inputCol=column_type[0], outputCol=column_type[0]+"_index",
                                          handleInvalid="skip") \
                        .fit(df_in) \
                        .transform(df_in)

                    # df_in = df_in.drop(column_type[0])
                    args['featureColNames'].remove(column_type[0])
                    args['featureColNames'].append(column_type[0]+"_index")

        # all features need to be vectors in a single column, usually named features
        vecAssembler = VectorAssembler(inputCols=args["featureColNames"], outputCol="features")
        # self.set_arg('featureColNames', 'features')
        args["featureColNames"] = "features"
        df_in = vecAssembler.transform(df_in)
    else:
        # self.set_arg('featureColNames', 'features')
        args["featureColNames"] = "features"
    return df_in

if __name__=="__main__":
    args = {"featureColNames": [], "labelColName": ['label']}

    spark = SparkSession.builder\
                .appName("addFeatures")\
                .master("local[1]")\
                .config("spark.debug.maxToStringFields",'500')\
                .getOrCreate()

    path = "E:\pythonProject\DPT\\files\dataset/"
    df = spark.read\
        .option("inferSchema","true")\
        .option("header","true")\
        .csv(path+"model03_train.csv")
    df.printSchema()
    print(df.count())
    df.show()


    #将timestamp类型转成string
    type_list = df.dtypes
    print(type_list)
    for i in type_list:
        tp = i[1]
        col_name = i[0]
        if tp == "timestamp":
           df =  df.withColumn(col_name,df[col_name].cast("string"))
           df = df.na.fill({col_name:" "})
        if tp == "string":
            df = df.na.fill({col_name:" "})
        if tp == 'int' or tp == 'decimal(8,0)' or tp == 'double':
            df = df.na.fill({col_name:0})


    df.show()

    #向量化
    df = preProcess(df_in = df,args = args)

    #将label列移到最后
    label = args['labelColName']
    # label = 'label1'
    df = df.withColumn("label000",df[label])
    df = df.drop(label)
    df = df.withColumn(label,df["label000"])
    df = df.drop("label000")
    df.show()
    print(df.count())

    #保存
    df.write.parquet(path+"model03")

    print(args)