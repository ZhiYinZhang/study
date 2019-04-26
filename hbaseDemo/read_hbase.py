#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/4/1 16:43
import happybase
from happybase import Table
import happybase
from happybase import Table
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
        data[key] = data[key].decode()
    return [row[0],data]

def get(table_name,family,cols:list,upper_case=False,limit=100):
    conn = happybase.Connection(host=hbase["host"])
    table = conn.table(table_name)

    print(table_name, family)

    if upper_case:
        # 转为大写
        for i in range(len(cols)):
            col = cols[i].upper()
            cols[i] = f"{family}:{col}"

    print(cols)
    rows = table.scan(columns=cols, limit=limit)

    return rows

def delete(table_name,family,cols:list,upper_case=False):
    conn = happybase.Connection(host=hbase["host"])
    table = conn.table(table_name)

    print(table_name, family)

    if upper_case:
        # 转为大写
        for i in range(len(cols)):
            col = cols[i].upper()
            cols[i] = f"{family}:{col}"

    print(cols)

    rows = table.scan(columns=cols)

    with table.batch(batch_size=500) as batch:
            for row in rows:
                print(row[0],row[1])
                batch.delete(row[0],cols)


from  random import randint
if __name__=="__main__":
    tables=["member3","TOBACCO.AREA","TOBACCO.RETAIL","TOBACCO.RETAIL_WARNING","test_ma"]
    hbase["table"]=tables[3]

    hbase["families"] = "0"
    # hbase["families"] = "column_A"
    # hbase["families"]="info"

    hbase["row"]="sale_center_id"
    # hbase["row"] = "cust_id"

    conn=happybase.Connection(host=hbase["host"])
    table=conn.table(hbase["table"])



    # table.put(row="encode",data={"column_A:cust_id":"hbase 中的列 与dataFrame中的列对应".encode()})
    # for i in range(10):
    #    table.put(row=f"{i}",data={"0:LICENSE_CODE":f"{randint(0,1000)}"})

    cols = ["grade_abno"]
    rows=get(table_name=hbase["table"], family=hbase["families"], cols=cols, upper_case=True,limit=1000)
    for row in rows:
            print(row)
            # print(decode(row,family=hbase["families"],cols=["cust_name"],upper_case=True))




    # cols=["order_ip_addr"]
    # delete(table_name=hbase["table"],family=hbase["families"],cols=cols,upper_case=True)