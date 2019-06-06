#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/5/29 15:51
from pyspark.sql import SparkSession
import json
"""
hbase-client-1.2.6.jar
hbase-server-1.2.6.jar
hbase-common-1.2.6.jar
hbase-protocol-1.2.6.jar
metrics-core-2.2.0.jar 
spark-examples_2.11-1.6.0-typesafe-001.jar
放到D:\Anaconda3\Lib\site-packages\pyspark\jars目录下
"""

def handler(data):
    """
    :param data: ('0','{"qualifier" : "age", "timestamp" : "1559025649145", "columnFamily" : "0", "row" : "0", "type" : "Put", "value" : "11"}\n{"qualifier" : "name", "timestamp" : "1559025649145", "columnFamily" : "0", "row" : "0", "type" : "Put", "value" : "Tom0"}')
    :return:
    """
    values = data[1].split("\n")
    row = {}
    for value in values:
        v = json.loads(value)

        row["columnFamily"] = v["columnFamily"]
        row["row"] = v["row"]
        row["timestamp"] = v["timestamp"]
        row["type"] = v["type"]

        row[v["qualifier"]] = bytes(v["value"],encoding="utf-8").decode(encoding="utf-8")

    return row


def rdd_to_df(hbase_rdd):
    """
        [
        ('0', '{"qualifier" : "age", "timestamp" : "1559025649145", "columnFamily" : "0", "row" : "0", "type" : "Put", "value" : "11"}\n{"qualifier" : "name", "timestamp" : "1559025649145", "columnFamily" : "0", "row" : "0", "type" : "Put", "value" : "Tom0"}'),
        ('1', '{"qualifier" : "age", "timestamp" : "1559025649145", "columnFamily" : "0", "row" : "1", "type" : "Put", "value" : "88"}\n{"qualifier" : "name", "timestamp" : "1559025649145", "columnFamily" : "0", "row" : "1", "type" : "Put", "value" : "Tom1"}')
        ]
    :param hbase_rdd:
    """
    rdd1 = hbase_rdd.map(handler)

    df = spark.createDataFrame(rdd1)

    # take(1)=[['columnFamily':'', 'row':'', 'timestamp':'', 'type':'', 'age':'', 'name':'']]
    # colNames=[x for x in rdd1.take(1)[0][0]]
    # df = rdd1.map(lambda x: [x[0][i] for i in x[0]]).toDF(colNames)

    return df
if __name__=="__main__":
    hbase_dpd="E://test/hbase_dpd/*"
    spark = SparkSession.builder.appName("spark hbase") \
        .master("local[2]") \
        .config("spark.driver.extraClassPath", hbase_dpd) \
        .config("spark.executor.extraClassPath", hbase_dpd) \
        .getOrCreate()
    sc = spark.sparkContext

    host = '10.72.59.89'
    table = 'TOBACCO.AREA'
    conf = {"hbase.zookeeper.quorum": host,
            "hbase.mapreduce.inputtable": table,
            "hbase.client.scanner.timeout.period":"120000"
            }
    keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"
    """
    [
    ('0', '{"qualifier" : "age", "timestamp" : "1559025649145", "columnFamily" : "0", "row" : "0", "type" : "Put", "value" : "11"}\n{"qualifier" : "name", "timestamp" : "1559025649145", "columnFamily" : "0", "row" : "0", "type" : "Put", "value" : "Tom0"}'),
    ('1', '{"qualifier" : "age", "timestamp" : "1559025649145", "columnFamily" : "0", "row" : "1", "type" : "Put", "value" : "88"}\n{"qualifier" : "name", "timestamp" : "1559025649145", "columnFamily" : "0", "row" : "1", "type" : "Put", "value" : "Tom1"}')
    ]
    """
    hbase_rdd = sc.newAPIHadoopRDD("org.apache.hadoop.hbase.mapreduce.TableInputFormat",
                                   "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
                                   "org.apache.hadoop.hbase.client.Result",
                                   keyConverter=keyConv,
                                   valueConverter=valueConv,
                                   conf=conf)

    df=rdd_to_df(hbase_rdd)
    df.show()

