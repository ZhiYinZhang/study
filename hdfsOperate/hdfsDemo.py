#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/10/10 16:53
from hdfs import *
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import hdfs
import json
import time

hdfsHost="http://entrobus11:50070"
user = "zhangzy"
cli = client.InsecureClient(url=hdfsHost,user=user,root='/user/zhangzy')


# cli.upload(hdfs_path="DPT/temp/123321/EA96AF677276F9DA54865C8EA8F8AA61",local_path="e://pythonProject/DPT/files/123321/dataset/words.csv")
def read_hdfs(file_path):

    start = time.time()
    with cli.read(file_path) as file:
        content =json.loads(file.read())
    end = time.time()
    print(f"read time:{end-start}")
    return content

def write_hdfs(file_path,data):
    start = time.time()
    cli.write(hdfs_path=file_path,data=json.dumps(data),overwrite=True,replication=1)
    end = time.time()
    print(f"write time:{end-start}")
if __name__=="__main__":
    # # content = read_hdfs("DPT/temp/test/multiCol.json")
    # # write_hdfs("DPT/temp/test/1.json",content)
    # df_pd = pd.DataFrame(data=np.random.randn(10,4),columns=['a','b','c','d'])
    # spark = SparkSession.builder.appName('test').master("local[2]").getOrCreate()
    # df = spark.createDataFrame(df_pd)
    # # df.write.csv("")
    # cli.write(hdfs_path='DPT/temp/test/spark.csv',data=df.write.csv("DPT/temp/test/spark.csv"))
    # # cli.upload(hdfs_path="DPT/dataset",
    # #            local_path="e://pythonProject/DPT/files/123321/dataset/multiCol.csv")
    # with cli.read(hdfs_path='DPT/dataset/model04_train_03.parquet') as reader:
    #     df = pd.read_parquet(reader)
    # print(df[:])
    # cli.delete("DPT/dataset/model01_train.parquet")


    cli.upload(hdfs_path= 'DPT/dataset',local_path='E:\pythonProject\dataset/model06_test.parquet',overwrite=True)