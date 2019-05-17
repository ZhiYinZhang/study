#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/4/15 11:49
import happybase as hb
import pandas as pd
from happybase import Table
import pdhbase as pdh

host="10.72.32.26"


def pd_read_hbase0(table,families):
    """

    :param table:
    :param families: ["column_family"]
    :return:
    """
    # coordinate 只有列族   一个列族对应一个值
    # table = "coordinate"
    # families=["citycode","cityname","longitude","latitude","type_code","types"]

    conn = hb.Connection(host=host)
    table = conn.table(table)

    rows = table.scan(columns=families, limit=1000)

    df = pd.DataFrame(columns=families)
    for key, value in rows:
        df_row = {k.decode().split(":")[0]: value[k].decode() for k in value.keys()}
        df = df.append(df_row, ignore_index=True)
    return df

def pd_read_hbase1(table,family,cols,upper=False,limit=1000):
    """

    :param table: 表名
    :param fly_col: ["column_family:column"]
    :return:
    """

    if upper:
        # 转为大写
        for i in range(len(cols)):
            col = cols[i].upper()
            cols[i]=col

    #拼接 family:column
    fly_cols = []
    for col in cols:
        fly_col = f"{family}:{col}"
        fly_cols.append(fly_col)

    print(cols)
    print(fly_cols)
    #获取连接
    conn = hb.Connection(host=host)
    table = conn.table(table)

    #获取数据
    rows = table.scan(columns=fly_cols, limit=limit)

    df_rows=[]
    df = pd.DataFrame(columns=cols)
    for key, value in rows:
        #value  {b'0:col1':'value'}
        df_row = {k.decode().split(":")[1] : value[k].decode() for k in value.keys()}
        df_row["ID"]=key.decode()
        # print(df_row)
        df_rows.append(df_row)

        if len(df_rows)==(limit/10):
              df = df.append(df_rows, ignore_index=True)
              df_rows.clear()
    # print(df)
    return df

def pd_write_hbase(df: pd.DataFrame, cols: list):
    """
    :param df:  pandas DataFrame
    :param cols: 除row_key之外
    """
    host = "10.72.32.26"
    table = "member3"
    row = "sale_center_id"
    family = "column_A"

    pool = hb.ConnectionPool(host=host, size=10)
    with pool.connection() as conn:
        table: Table = conn.table(table)
        try:
            with table.batch(batch_size=1000) as batch:
                for x in range(len(df)):
                    l = df.iloc[x][cols].values

                    row_key = df.iloc[x][row]  # row key

                    data = {}
                    for y in range(len(cols)):
                        fly_col = f"{family}:{cols[y]}"
                        data[fly_col] = str(l[y])
                    batch.put(row=str(row_key), data=data)
        except Exception as e:
            print(e.args)
if __name__=="__main__":
    table="TOBACCO.RETAIL"
    # fly_col=["0:PRICE_LAST_MONTH","0:PRICE_LAST_FOUR_WEEK"]
    cols=["id","grade","status","city","county"]
    family=0
    df=pd_read_hbase1(table, family, cols, upper=True,limit=1000)
    print(df)

    # table="coordinate"
    # families=["citycode","cityname","longitude","latitude","type_code","types"]
    # df=pd_read_hbase0(table,families)
    # print(df)