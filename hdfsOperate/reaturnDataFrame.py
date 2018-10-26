#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/10/12 10:37
from hdfs import client
import pandas as pd

HDFSHOST = "http://entrobus11:50070"
userName = 'zhangzy'
root = '/user/zhangzy'
col = ['xx']
cli = client.InsecureClient(url=HDFSHOST,user=userName,root=root)
def readHDFS(filePath):
    '''
    读取hdfs文件
    Returns：
    df:dataframe hdfs数据
    '''

    # 目前读取hdfs文件采用方式：
    # 1. 先从hdfs读取二进制数据流文件
    # 2. 将二进制文件另存为.csv
    # 3. 使用pandas读取csv文件

    with cli.read(filePath) as fs:
              df = pd.read_csv(fs)
    return df
def writeHDFS(filePath):
           pass
if  __name__=="__main__":
    d = {"evaluate": {'areaUnderROC': 0.7647349880387647, 'areaUnderPR': 0.7156326647102125}}
    r = d['evaluate']
    print(r.keys().__str__()[1:-1])
    print(r.values().__str__()[1:-1])

