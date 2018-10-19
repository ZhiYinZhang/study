#!/usr/bin/env python
# -*- coding:utf-8 -*-
from pyspark.sql import SparkSession,functions,Window
from pyspark.sql.functions import *
from pyspark.sql import Column
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.ml.feature import *
import json
import pika
import time
import pandas as pd
# 写回RabbitMQ
def write_to_mq(res_data : dict):
    # rabbitmq的配置
    # user = "bigdata"
    # passwd = "bigdata"
    # exchange = "conn_test"
    # virtual_host = "bigdata_test"
    # port = 5672
    # host = "139.199.161.144"
    # route_key = "test.json.input"


    user = "cloudai"
    passwd = "cloudai"
    host = "139.199.161.144"
    port = 5672
    exchange = "sub_clean_result"
    # exchange="pub_data_clean"
    virtual_host = "cloudai"
    # queue_name='pub_data_clean'
    queue_name = 'sub_clean_result'
    route_key = 'data_clean'


    res_content = json.dumps(res_data)
    # TODO RabbitMQ配置被写死，将其写入配置文件中
    credential = pika.PlainCredentials(user,passwd)

    # 连接RabbitMQ
    with pika.BlockingConnection(pika.ConnectionParameters(host=host,
                                                           credentials=credential,
                                                           virtual_host=virtual_host,
                                                           port=port
                                                           )) as connection:
        # 定义channel
        channel = connection.channel()
        # declare exchange
        channel.exchange_declare(exchange=exchange,
                                 exchange_type="topic",
                                 durable=True
                                 )
        channel.queue_declare(queue=queue_name)
        channel.queue_bind(queue=queue_name,exchange=exchange,routing_key=route_key)
        # publish message route by test.json.output
        channel.basic_publish(exchange=exchange,
                              routing_key=route_key,
                              body=res_content)
        connection.close()

def sparkDemo():


    start=time.time()
    spark=SparkSession \
        .builder \
        .appName("readFile") \
        .master("local[3]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df=spark.read \
         .format("json") \
         .option("inferSchema","true") \
         .option("path","E:/test/checkpoint/spark.json") \
         .load()
    start=time.time()

    list_row=df.take(100)
    list_json=[]
    for i in list_row:
        d=i.asDict()
        list_json.append(d)
    print(list_json)
    path = "e:/test/checkpoint/test1.json"
    with open(path,"w") as file:
        json.dump(list_json,file)


    end=time.time()

    print(f"time:{'%.3f'%(end-start)}s")

if __name__=="__main__":
    i=0
    sparkDemo()
