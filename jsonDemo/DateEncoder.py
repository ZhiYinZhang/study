#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/10/30 11:23
import json
import datetime
import hdfs
import pandas as pd
from pandas import DataFrame
from pandas._libs.tslibs.timestamps import *
"""
自定义json模块的encoder
"""
class DateEncoder(json.JSONEncoder):
    def default(self,obj):
        if isinstance(obj,datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj,datetime.date):
            return obj.strftime("%Y-%m-%d")
        else:
            return json.JSONEncoder.default(self,obj)

cli = hdfs.InsecureClient(url='http://10.18.0.11:50070',user='zhangzy',root='/user/zhangzy')
if __name__=="__main__":
    d = {'a':datetime.datetime.now()}
    # j = json.dumps(d,cls=DateEncoder)
    # # print(j)
    #
    # with cli.read('test2.json') as reader:
    #     df:DataFrame = pd.read_json(reader)
    #
    # d = df.to_dict(orient='record')
    # print(type(d[0]['timestamp']))

    j = json.dumps(d,cls=DateEncoder)
    print(j)