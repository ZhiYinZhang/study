#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/10/23 14:09
import pandas as pd
from numpy import dtype

path = "e://pythonProject/dataset/"
df = pd.read_csv(filepath_or_buffer=path+'model03_train.csv')


# print(df['Var200'].drop_duplicates().count())
df['label'] = df['label'].replace(to_replace=-1,value=0)

df = df.fillna(value=0)
df = df.fillna('0')
df.to_csv(path+'model03_train_fill.csv',index=False)


