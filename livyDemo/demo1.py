#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/11/6 13:53
import json
import requests
from requests.models import Response
import pprint
import textwrap

def create():
    data = {#"kind": "pyspark",  # 定义代码类型
            "pyFiles": ["hdfs://localhost:8020/user/zhangzy/DPT/lib.zip"],  # list of string
            "executorMemory": "2G",
            "executorCores": 2,
            "numExecutors": 3
            }
    # 创建一个spark应用  POST livy_host/sessions
    rp = requests.post(url=livy_host + "/sessions", data=json.dumps(data), headers=headers)
    return rp
def submit(session_url,code:str):
    """

    :param session_url: /sessions/{sessionId}
    :param code: string
    :return:
    """
    statements_url = livy_host+session_url+"/statements"
    code = textwrap.dedent(f"{code}")
    body = {"kind":"pyspark","code":code}
    # 提交代码块
    rp_st = requests.post(url=statements_url, data=json.dumps(body), headers=headers)
    return rp_st

def get_status(response:Response):
    # 查看spark应用状态  GET livy_host/sessions/{sessionId}
    # 查看程序运行状态   GET livy_host/sessions/{sessionId}/statements/{statementId}
    location = response.headers.get('location')
    if location:
        status_url = livy_host + response.headers['location']
        print(f"status_url:{status_url}")
        status = requests.get(url=status_url, headers=headers)
        pprint.pprint(status.json())
    else:
        pprint.pprint(response.json())

if __name__=="__main__":
    livy_host = "http://10.18.0.11:8998"
    headers = {"Content-Type": "application/json"}

    # rp = create()
    # get_status(rp)

    code = """
    df = spark.read.format("csv").option("header","true").option("inferSchema","true").option("path","hdfs://10.18.0.11:8020/user/zhangzy/DPT/dataset/model01_train.csv").load()
    df.printSchema()
    df.show(truncate=False)
    """
    rp = submit("/sessions/13",code)
    pprint.pprint(rp.headers)

    # result = requests.get(url="http://10.18.0.11:8998/sessions/13/statements/1")
    # pprint.pprint(result.json())