#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/4/1 16:43
import happybase
from happybase import Table
from datetime import datetime as dt
hbase={"host":"10.72.32.26","size":10,"table":"member3",
       "row":"cust_id",
       "familys":["column_A","column_B"],  #本次要写的列族
       "column_A":["4","5"],"column_B":["1","2"], #要写的列族中的列
        "1":"sum(qty_sum)","2":"sum(qty_sum)","4":"sum(qty_sum)","5":"sum(qty_sum)" # hbase的列 对应 DataFrame的列
       }

#
def write_hbase(rows):
        pool=happybase.ConnectionPool(host=hbase["host"],size=hbase["size"])
        with pool.connection() as conn:
               table:Table=conn.table(hbase["table"])
               try:
                   with table.batch(batch_size=1000) as batch:
                       for row in rows:
                           row_key=row[hbase["row"]]#row key
                           for family in hbase["familys"]:#列族
                                for col in hbase[family]:#列
                                     fly_col=f"{family}:{col}"
                                     df_col=hbase[col] #hbase 中的列 与dataFrame中的列对应
                                     batch.put(row=row_key,data={fly_col:str(row[df_col])})
               except Exception as e:
                   print(e.args)

def write_hbase1(rows, cols):
    """
    :param rows:  spark的DataFrame以foreachPartition方法  List[Row]
    :param cols:   DataFrame的列，直接以这个列名作为hbase的列名，所以要先将DataFrame的列名改成hbase中的列名
    :return:
    """
    pool = happybase.ConnectionPool(host=hbase["host"], size=hbase["size"])
    with pool.connection() as conn:
        table: Table = conn.table(hbase["table"])
        try:
            with table.batch(batch_size=1000) as batch:
                for row in rows:
                    row_key = row[hbase["row"]]  # row key
                    for family in hbase["familys"]:  # 列族
                        data = {}
                        for c in cols:  # 列
                            fly_col = f"{family}:{c.upper()}"
                            data[fly_col] = str(row[c])
                            # df_col=hbase[col] #hbase 中的列 与dataFrame中的列对应
                        batch.put(row=row_key, data=data)
        except Exception as e:
            print(e.args)
def decode(row,family,cols:list,upper_case=False):
    """

    :param row: hbase的一行  [rowKey,{}]
    :param keys: 需要decode的列
    """
    if upper_case:
        # 转为大写
        for i in range(len(cols)):
            col = cols[i].upper()
            cols[i] = bytes(f"{family}:{col}",encoding="utf-8")

    data = row[1]
    for key in cols:
        try:
          data[key] = data[key].decode()
        except Exception as e:
              pass
    return [row[0],data]

def get(table_name,family,cols:list,upper_case=False,limit=100):
    conn = happybase.Connection(host=hbase["host"])
    table = conn.table(table_name)

    print(table_name, family)

    if upper_case:
        # 转为大写
        for i in range(len(cols)):
            col = cols[i].upper()
            cols[i]=col


    # 拼接 family:column
    fly_cols = []
    for col in cols:
        fly_col = f"{family}:{col}"
        fly_cols.append(fly_col)

    print(fly_cols)
    rows = table.scan(columns=fly_cols, limit=limit)

    return rows

def delete(table_name,family,cols:list,upper_case=False):
    pool = happybase.ConnectionPool(host=hbase["host"], size=hbase["size"])
    with pool.connection() as conn:
        table = conn.table(table_name)

        print(table_name, family)

        if upper_case:
            # 转为大写
            for i in range(len(cols)):
                col = cols[i].upper()
                cols[i] = f"{family}:{col}"

        print(cols)

        rows = table.scan(columns=cols)
        with table.batch(batch_size=10000) as batch:
                i=0
                for row in rows:
                    # print(row[0],row[1])
                    batch.delete(row[0],cols)

def delete_all(table_name):
    pool = happybase.ConnectionPool(host=hbase["host"], size=hbase["size"])
    with pool.connection() as conn:
        table = conn.table(table_name)

        print(table_name)
        rows = table.scan()
        size=0
        with table.batch(batch_size=10000) as batch:
            for row in rows:
                size+=1
                print(row[0],row[1])
                batch.delete(row[0])
        print("size:",size)
from  random import randint
if __name__=="__main__":
    tables=["test1","test2",
            "TOBACCO.AREA","TOBACCO.RETAIL",
            "TOBACCO.RETAIL_WARNING","TOBACCO.WARNING_CODE",
            "TOBACCO.DATA_INDEX","TOBACCO.BLOCK_DATA"]
    hbase["table"]=tables[0]

    hbase["families"] = "0"

    # hbase["row"]="sale_center_id"
    # hbase["row"] = "cust_id"

    conn=happybase.Connection(host=hbase["host"])
    table=conn.table(hbase["table"])


    print(str(dt.now()))
    with table.batch(batch_size=1000) as batch:
        for i in range(0,10000):
            batch.put(row=f"{i}",data={"0:age":f"{randint(0,100)}","0:name":f"Tom{i}"})
    print(str(dt.now()))



    # cols = {"value": "stream_avg_orders",
    #         "level1_code": "classify_level1_code"
    #         }
    # values = [
    #          "avg_orders_plus3", "avg_orders_minu3", "avg_orders_plus4",
    #          "avg_orders_minu4", "avg_orders_plus5", "avg_orders_minu5",
    #          "warning_level_code", "cust_id", "city", "sale_center_id"
    #          ] + list(cols.values())
    #
    # cols = ["primary_industry_add"]
    # rows = get(table_name=hbase["table"], family="0", cols=cols, upper_case=True,limit=1300)
    # for row in rows:
    #         print(row)
            # print(decode(row,family=hbase["families"],cols=["in_prov_items","out_prov_items"],upper_case=True))



    # print(str(dt.now()))
    # cols=["one_id_more_retail","one_tel_more_retail","one_bank_more_retail","one_ip_more_retail","order_ip_addr"]
    # delete(table_name=hbase["table"],family=hbase["families"],cols=cols,upper_case=True)
    # print(str(dt.now()))


    print(str(dt.now()))
    delete_all(hbase["table"])
    print(str(dt.now()))