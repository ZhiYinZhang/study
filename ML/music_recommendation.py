#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/3/15 9:44
import pandas as pd
pd.set_option("display.max_columns",None)
path="E:/test/kkbox-music-recommendation-challenge/songs.csv"
df=pd.read_csv(path)

df.drop_duplicates()