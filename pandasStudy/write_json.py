#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/1/9 18:09

import pandas as pd

data=[
    ["a",1],
    ["b",2],
    ["数",3]
]
df=pd.DataFrame(data,columns=["c1","c2"])


print(df.head())

"""
orient=records
数据:[{"c1":"a","c2":1},{"c1":"b","c2":2}]
force_ascii=False，utf-8编码
"""
# df.to_json("e:/test/json/shaoshanshi.json",orient="records",force_ascii=False)

"""
orient=records,lines=True
数据:
{"c1":"a","c2":1}
{"c1":"b","c2":2}
spark写json也是这种格式
"""
# df.to_json("e:/test/json/shaoshanshi.json",orient="records",force_ascii=False,lines=True)



#指定编码,force_ascii也要指定为False
# with open("e:/test/json/shaoshanshi1.json",mode="w",encoding="GBK") as writer:
#     df.to_json(writer,orient="records",force_ascii=False)