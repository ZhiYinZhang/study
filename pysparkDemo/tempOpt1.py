#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import pandas as pd
import happybase
from happybase import Table
import json
def pd_write_hbase(df: pd.DataFrame, cols: list,param:dict):
    """
    :param df:  pandas DataFrame
    :param cols: 除row_key之外
    """
    host=param["host"]
    table=param["table"]
    row=param["row"]
    family=param["family"]


    pool = happybase.ConnectionPool(host=host, size=10)
    with pool.connection() as conn:
        table: Table = conn.table(table)
        try:
            with table.batch(batch_size=1000) as batch:
                for x in range(len(df)):
                    l = df.iloc[x][cols].values

                    row_key = df.iloc[x][row]  # row key

                    data = {}
                    for y in range(len(cols)):
                        fly_col = f"{family}:{cols[y].upper()}"
                        data[fly_col] = str(l[y])
                    batch.put(row=str(row_key), data=data)
        except Exception as e:
            print(e.args)


if __name__=="__main__":
    pass



