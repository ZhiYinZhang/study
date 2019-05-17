#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/4/8 16:35
import pandas as pd
import numpy as np
import happybase
from happybase import Table
import uuid
hbase_host="10.72.32.26"
hbase_pool_size=10


def write_hbase(rows,hbase):
    """
    spark row 的不同列写入不同family column
    :param rows:   list[Row]
    :param hbase
             hbase = {"table": "test","row": "id", "families": ["fly_col1","fly_col2"],
                  "fly_col1":["col1","col2"],"fly_col2":["col3","col4"]}
          row  DataFrame的那列作为hbase的row key
          families hbase表的列族，
          "fly_col1" 为fly_col1列族需要DataFrame的那些列   DataFrame的列名即为hbase的列名
    """
    pool = happybase.ConnectionPool(host=hbase_host, size=hbase_pool_size)
    with pool.connection() as conn:
        table: Table = conn.table(hbase["table"])
        try:
            with table.batch(batch_size=1000) as batch:
                for row in rows:
                    row_key = str(row[hbase["row"]])  # row key
                    data = {}
                    for family in hbase["families"]:  # 列族
                        for col in hbase[family]:    #每个列族 的列
                             fly_col=f"{family}:{col}"
                             data[fly_col]=str(row[col])
                    # data={"family1:col1":value1,"family1:col2":value2}
                    batch.put(row=row_key,data=data)
        except Exception as e:
            print(e.args)





def write_hbase1(rows,cols,hbase):
    """
      spark row中的cols包含的列 写入同一个family column
    :param rows:   list[Row]
    :param cols:   list[colName] 需要写入hbase的DataFrame的列 并且与hbase的列名对应
    :param hbase: {"table":hbase_tableName,"row":row_key,"families":[fly_col1]}
                   table  hbase中的表
                   row    DataFrame中为做row key的列
                   families hbase中的列族
    """
    pool = happybase.ConnectionPool(host=hbase_host, size=hbase_pool_size)
    with pool.connection() as conn:
        table: Table = conn.table(hbase["table"])
        try:
            with table.batch(batch_size=1000) as batch:
                for row in rows:
                    data = {}
                    row_key = str(row[hbase["row"]])  # row key
                    for family in hbase["families"]:  # 列族
                        for c in cols:  # 列
                            fly_col = f"{family}:{c.upper()}"
                            data[fly_col] = str(row[c])
                        # data={"family1:col1":value1,"family1:col2":value2}
                        batch.put(row=row_key, data=data)
        except Exception as e:
            print(e.args)
def write_hbase2(rows,cols,hbase):
    """
      spark row中的cols包含的列 写入同一个family column
    :param rows:   list[Row]
    :param cols:   list[colName] 需要写入hbase的DataFrame的列 并且与hbase的列名对应
    :param hbase: {"table":hbase_tableName,"row":row_key,"families":[fly_col1]}
                   table  hbase中的表
                   row    DataFrame中为做row key的列
                   families hbase中的列族
    """
    pool = happybase.ConnectionPool(host=hbase_host, size=hbase_pool_size)
    with pool.connection() as conn:
        table: Table = conn.table(hbase["table"])
        try:
            with table.batch(batch_size=1000) as batch:
                for row in rows:
                    data = {}
                    row_key=str(uuid.uuid1()).replace("-","")
                    for family in hbase["families"]:  # 列族
                        for c in cols:  # 列
                            fly_col = f"{family}:{c.upper()}"
                            data[fly_col] = str(row[c])
                        # data={"family1:col1":value1,"family1:col2":value2}
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


if __name__=="__main__":
    pass
    # df=pd.read_csv("E:/test_return(1).csv")
    # print(df)
    # pd_write_hbase(df,["sarima_weightd_overall_alarming","historical_overall_alarming"])
