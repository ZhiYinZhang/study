#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/4/8 16:35
import pandas as pd
import numpy as np
import happybase
from happybase import Table

hbase = {"host": "10.72.32.26", "size": 10, "table": "member3",
         "row": "cust_id", "families": ["column_A"]}

def write_hbase1(rows, cols):
    pool = happybase.ConnectionPool(host=hbase["host"], size=hbase["size"])
    with pool.connection() as conn:
        table: Table = conn.table(hbase["table"])
        try:
            with table.batch(batch_size=1000) as batch:
                for row in rows:
                    row_key = row[hbase["row"]]  # row key
                    for family in hbase["families"]:  # 列族
                        data = {}
                        for c in cols:  # 列
                            fly_col = f"{family}:{c.upper()}"
                            data[fly_col] = str(row[c])
                        batch.put(row=row_key, data=data)
        except Exception as e:
            print(e.args)



def pd_write_hbase(df:pd.DataFrame,cols:list):
        """
        :param df:  pandas DataFrame
        :param cols: 除row_key之外
        """
        host="10.72.32.26"
        table="member3"
        row="sale_center_id"
        family="column_A"

        pool=happybase.ConnectionPool(host=host,size=10)
        with pool.connection() as conn:
            table: Table = conn.table(table)
            try:
                with table.batch(batch_size=1000) as batch:
                    for x in range(len(df)):
                        l = df.iloc[x][cols].values

                        row_key=df.iloc[x][row] # row key

                        data = {}
                        for y in range(len(cols)):
                            fly_col = f"{family}:{cols[y]}"
                            data[fly_col] = str(l[y])
                        batch.put(row=str(row_key), data=data)
            except Exception as e:
                print(e.args)

df=pd.read_csv("E:/test_return(1).csv")
print(df)
pd_write_hbase(df,["sarima_weightd_overall_alarming","historical_overall_alarming"])

