#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/11 14:59
import numpy as np
import os
import pandas as pd
import json
if __name__=="__main__":
    data={'dt': '{"min(dt)":"1900-01-01","max(dt)":"1900-01-01"}',
     'null_ratio': '{"id_": 0.0, "brdowner_id": 0.0, "brdowner_name": 0.0, "addr": 97.5, "short_name": 0.0, "note": 100.0, "seq": 0.0, "brdowner_img": 97.5, "brdowner_img1": 97.5, "operation_desc": 100.0, "owner_mrb": 0.0, "industry_id": 100.0, "brdowner_img2": 97.5, "brdowner_img3": 97.5, "operation_desc1": 100.0, "longitude": 0.0, "latitude": 0.0, "dt": 0.0}',
     'summary': '{"count": {"id_": "40", "seq": "40", "longitude": "40", "latitude": "40"}, "mean": {"id_": "20.5", "seq": "1.0", "longitude": "0E-12", "latitude": "0E-12"}, "stddev": {"id_": "11.69045194450012", "seq": "0.0", "longitude": "0.0", "latitude": "0.0"}, "min": {"id_": "1", "seq": "1", "longitude": "0E-8", "latitude": "0E-8"}, "25%": {"id_": "10", "seq": "1", "longitude": "0.0", "latitude": "0.0"}, "50%": {"id_": "20", "seq": "1", "longitude": "0.0", "latitude": "0.0"}, "75%": {"id_": "30", "seq": "1", "longitude": "0.0", "latitude": "0.0"}, "max": {"id_": "40", "seq": "1", "longitude": "0E-8", "latitude": "0E-8"}}',
     'value_count': '{"brdowner_id":40,"brdowner_name":38,"addr":4,"short_name":38,"note":1,"brdowner_img":2,"brdowner_img1":2,"operation_desc":1,"owner_mrb":2,"industry_id":1,"brdowner_img2":2,"brdowner_img3":2,"operation_desc1":1}'}
    df=pd.DataFrame(data=[data],index=["table1"])
    for i in df.columns:
        print(df[i])
    # df.to_excel("e://test//text.xlsx",index_label=["tableName "])