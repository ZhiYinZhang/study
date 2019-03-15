#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/3/13 18:14
import pandas as pd
import json
import os
import numpy as np
pd.set_option("display.max_columns",None)
yanpk=pd.read_csv('E:/test/yanpk20190312.csv')
yanpk['name'] = yanpk['name'].apply(lambda x: json.loads(x))
yanpk['attr'] = yanpk['attr'].apply(lambda x: json.loads(x))
yanpk['scores'] = yanpk['scores'].apply(lambda x: json.loads(x))
yanpk['statistics'] = yanpk['statistics'].apply(lambda x: json.loads(x))

"""
{'小盒条形码：': '6901028178815', '条包条形码：': '6901028178822', '类型：': '烤烟型',
  '焦油含量：': '8 mg', '烟气烟碱量：': '0.8 mg', 'CO含量：': '9 mg', '烟长：': '74 mm', 
  '过滤嘴长：': '25 mm', '包装形式：': '条盒硬盒中支', '单盒支数：': '20', '销售形式：': '国产内销', '生产状态：': '待上市'}
"""

attr_keys=yanpk["attr"][0].keys()
for attr_key in attr_keys:
    yanpk[attr_key] = yanpk["attr"].apply(lambda x: x.get(attr_key,0))
# yanpk["小盒条形码"]=yanpk["attr"].apply(lambda x:x.get('小盒条形码：'))

print(yanpk["attr"])


