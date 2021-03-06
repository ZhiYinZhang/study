#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/4/1 16:43
import happybase
from happybase import Table
from datetime import datetime as dt

def write_hbase(rows,hbase):
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

def write_hbase1(rows,hbase,cols):
    """
    :param rows:  spark的DataFrame以foreachPartition方法  List[Row]
    :param hbase: {"table":hbase_tableName,"row":row_key,"families":[fly_col1]}
                   table  hbase中的表
                   row    DataFrame中为row key的列
                   families hbase中的列族
    :param cols:   DataFrame的列，直接以这个列名作为hbase的列名，所以要先将DataFrame的列名改成hbase中的列名
    :return:
    """
    pool = happybase.ConnectionPool(host=hbase["host"], size=hbase["size"])
    with pool.connection() as conn:
        table: Table = conn.table(hbase["table"])
        try:
            with table.batch(batch_size=1000) as batch:
                for row in rows:
                    row_key = str(row[hbase["row"]])  # row key
                    for family in hbase["families"]:  # 列族
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

def get(table_name,family,cols:list,upper_case=False,limit=100,row_prefix=None):
    conn = happybase.Connection(host=hbase["host"])
    table = conn.table(table_name)
    print(f"table name:{table_name},family column:{family}")

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

    print(f"cols:{fly_cols}")
    rows = table.scan(columns=fly_cols, limit=limit,row_prefix=row_prefix)

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

def delete_all(table_name,columns):
    """

    :param table_name: 表名
    :param column:  注意大小写
    """
    pool = happybase.ConnectionPool(host=hbase["host"], size=hbase["size"])
    with pool.connection() as conn:
        table = conn.table(table_name)

        print(table_name)


        for i in range(len(columns)):
               columns[i]=f"0:{columns[i]}"

        rows = table.scan(columns=columns)
        size=0
        with table.batch(batch_size=10000) as batch:
            for row in rows:
                size+=1
                print(row)
                # print(row[0],row[1])
                batch.delete(row[0])
        print("size:",size)

def upper_case(cols):
    result=[]
    for i in cols:
        result.append(i.upper())
    return result

from  random import randint
hbase={"host":"10.72.59.89","size":10,"table":"member3",
       "row":"cust_id",
       "families":["column_A","column_B"],  #本次要写的列族
       "column_A":["4","5"],"column_B":["1","2"], #要写的列族中的列
        "1":"sum(qty_sum)","2":"sum(qty_sum)","4":"sum(qty_sum)","5":"sum(qty_sum)" # hbase的列 对应 DataFrame的列
       }

if __name__=="__main__":
    prefix="V530_TOBACCO."
    tables=["TEST","test1",
            prefix+"AREA",prefix+"RETAIL",
            prefix+"RETAIL_WARNING",prefix+"WARNING_CODE",
            prefix+"DATA_INDEX",prefix+"BLOCK_DATA",
            prefix+"CIGA_PICTURE",prefix+"GEARS_TOSS",
            prefix+"CIGA_GRADE"]


    hbase["table"]=tables[3]
    hbase["families"] = "0"

    # hbase["row"]="sale_center_id"
    # hbase["row"] = "cust_id"

    # hbase["table"]="V530_TOBACCO.CODE"

    conn=happybase.Connection(host=hbase["host"])
    table=conn.table(hbase["table"])

    #查询有哪些表
    # tables=conn.tables()
    # for t in tables:
    #     print(t.decode("utf-8"))


    # table=conn.table(hbase["table"])
    # table = conn.table("V630_TOBACCO.GEARS_TOSS")
    # rows=table.scan(row_prefix=bytes("YJFL004","utf-8"))
    # # print(type(rows))
    # for row in rows:
    #     print(row)

    # table.put(row="01111430206",data={"0:COUNTY":"['渌口区']"})




    #写数据

    # print(str(dt.now()))
    # with table.batch(batch_size=1000) as batch:
    #     for i in range(5,10):
    #         batch.put(row=f"{i}",data={"0:CITY":f"{randint(0,1)}","0:COUNTY":f"Tom{i}","0:COM_ID":f"{i}"})
    # print(str(dt.now()))



    #读数据
    # cols = ["cust_id","city","county"]
    """
    rows=[
          [b"row_key1",{b"cf:col1":b"data",b"cf:col2":b"data"}],
          [b"row_key2",{b"cf:col1":b"data",b"cf:col2":b"data"}]
         ]
    """
    # rows = get(table_name=hbase["table"], family="0", cols=cols, upper_case=True,limit=1000,row_prefix=None)
    # for row in rows:
    #         print(row[0],row[1])
            # print(decode(row,family=hbase["families"],cols=["county"],upper_case=True))



    # print(str(dt.now()))
    # cols=["gauge_prop_like_ciga"]
    # delete(table_name=hbase["table"],family=hbase["families"],cols=cols,upper_case=True)
    # print(str(dt.now()))


    # print(str(dt.now()))
    # delete_all("V530_.RENT_INFO",[])
    # print(str(dt.now()))