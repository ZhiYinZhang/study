#!/usr/bin/env python
# -*- coding:utf-8 -*-

import pandas as pd

# data_str=open("e:\\test\\json\\part-00000-1b5cd992-919a-4d2e-8152-2a5bf5c04e97-c000.json",'r+')
df=pd.read_json(path_or_buf="e:\\test\\json\\part-00000-1b5cd992-919a-4d2e-8152-2a5bf5c04e97-c000.json",lines=True)


df1=pd.DataFrame(data=[[0,2,5],[0,3,6],[9,5,2]],index=[4,5,6],columns=['a','b','c'])
print (df1)
# df1.loc[:,'a']=10
print (df1.groupby(by=['a']).mean())
df.to_json('e:\\test\\json\\test.json',orient='records',lines=True)