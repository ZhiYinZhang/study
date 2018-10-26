#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/10/19 10:01
from pyspark.sql import SparkSession,DataFrame
from pyspark.ml.feature import VectorAssembler,StringIndexer
import time
import sys
from pyspark.sql.column import Column
def preProcess(df_in,args):

    col = df_in.columns

    print(col)
    # if len(args["labelColName"]) == 0 or args["labelColName"][0] not in col:
    #     if 'label' in col:
    #         args["labelColName"] = "label"
    #         col.remove('label')
    #     else:
    #         lab = col[-1]
    #         args['labelColName'] = lab
    #         col.remove(lab)
    #     args['featureColNames'] = col
    # else:
    #     # 把labelColName list->string
    #     args['labelColName'] = args['labelColName'][0]
    #     if args['featureColNames'] not in col:
    #         # col.remove(args['labelColName'])
    #         args["featureColNames"] = col
    args["featureColNames"] = col
    if "features" not in col:
        # index the string label    column_type: [(col1,col1Type),(col2,col2Type)]
        for column,col_type in df_in.dtypes:
            if column not in args["labelColName"]:

                if col_type == 'string':

                    df_in = StringIndexer(inputCol=column, outputCol=column+"_index",
                                          handleInvalid="error") \
                        .fit(df_in) \
                        .transform(df_in)

                    # df_in = df_in.drop(column_type[0])
                    args['featureColNames'].remove(column)
                    args['featureColNames'].append(column+"_index")

        # df_in.show()
        # df_in.write.csv(path='e://pythonProject//dataset//model03_test',mode='overwrite')

        # all features need to be vectors in a single column, usually named features
        print(args['featureColNames'])
        vecAssembler = VectorAssembler(inputCols=args["featureColNames"], outputCol="features")
        # self.set_arg('featureColNames', 'features')
        args["featureColNames"] = "features"
        df_in = vecAssembler.transform(df_in)
    else:
        # self.set_arg('featureColNames', 'features')
        args["featureColNames"] = "features"
    return df_in


def typeTransform(df: DataFrame, col, to_type):
    df = df.withColumn(col, df[col].cast(to_type))
    return df
if __name__=="__main__":
    args = {"featureColNames": [], "labelColName": []}

    spark = SparkSession.builder\
                .appName("addFeatures")\
                .master("local[1]")\
                .config("spark.debug.maxToStringFields",'500')\
                .getOrCreate()

    path = "E:\pythonProject/dataset/"
    df = spark.read\
        .option("inferSchema","true")\
        .option("header","true")\
        .csv(path+"model06_test.csv")
    df.printSchema()
    df.show(truncate=False)

    # df = df.withColumn('label',(df.label+1)/2)
    # df.show(truncate=False)
    df = typeTransform(df, '2_total_fee', 'double')
    df = typeTransform(df, '3_total_fee', 'double')
    # df = typeTransform(df, 'age', 'int')
    # df = typeTransform(df, 'gender', 'int')

    df = df.drop('user_id')
    # df = StringIndexer(inputCol='current_service', outputCol="label",
    #                    handleInvalid="error") \
    #     .fit(df) \
    #     .transform(df)
    # df = df.drop('current_service')

    df.printSchema()
    # #缺失值填充
    type_list = df.dtypes
    for col_name,tp in type_list:
        if tp == "timestamp":
           df = df.withColumn(col_name,df[col_name].cast("string"))
           df = df.na.fill({col_name:'0'})
        elif tp == "string":
            df = df.na.fill({col_name:'0'})
        else:
            # print(tp)
            df = df.na.fill({col_name:0})
    df.printSchema()
    df.show()



    #向量化
    df = preProcess(df_in=df, args=args)

    # df.show()
    #将label列移到最后
    # label = args['labelColName']
    # label = 'label1'
    # df = df.withColumn("label000",df[label])
    # df = df.drop(label)
    # df = df.withColumn(label,df["label000"])
    # df = df.drop("label000")

    df.show(truncate=False)


    #保存
    df.write.parquet(path+"model06_test",mode="overwrite")

    print(args)