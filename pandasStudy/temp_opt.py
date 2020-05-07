#-*- coding: utf-8 -*-
# @Time    : 2019/3/9 14:37
# @Author  : Z
# @Email   : S
# @File    : temp_opt.py
import pandas as pd
import json

# df.to_json(,orient="records",force_ascii=False)

# path="e:/test/json/shaoshanshi.json"
#
# df=pd.read_json(path,orient="records",lines=True)
#
# print(df)
# df.to_json("e:/test/json/shaoshanshi.csv",orient="records",force_ascii=False)


# df=pd.read_csv("E:/test/dianshang/data/cust_tel_20200110.csv",dtype=str)
#
# df.to_json("e://test/dianshang/data/cust_tel_20200110.json",orient="records")



# path="e://test//json//"
# df=pd.read_json(path+"part.json",orient="records",lines=True,encoding="utf-8",dtype=False)
#
#
# # pd.read_csv()
#
# print(df.dtypes)
#
# print(df)
# df.to_json(path+"part1.json",orient="records",force_ascii=False)

pd.read_excel()
df=pd.read_csv("e://test//csv//test.csv",dtype=str)

print(df)
print(df.dtypes)
