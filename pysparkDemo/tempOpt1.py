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
   # data=[[1,11,"tom"],[2,22,"jack"],[3,21,"xm"]]
   # df=pd.DataFrame(data,columns=["id","age","name"])
   # param={"host":"10.72.59.89","table":"test2","row":"id","family":"0"}
   #
   # pd_write_hbase(df,["age","name"],param)

   t=(' 011114310',
     '{"qualifier" : "AREA_INDEX", "timestamp" : "1559561950563", "columnFamily" : "0", "row" : " 011114310", "type" : "Put", "value" : "0.9375"}\n{"qualifier" : "CITY", "timestamp" : "1559561924550", "columnFamily" : "0", "row" : " 011114310", "type" : "Put", "value" : "\\\\xE9\\\\x83\\\\xB4\\\\xE5\\\\xB7\\\\x9E\\\\xE5\\\\xB8\\\\x82"}\n{"qualifier" : "COUNTY", "timestamp" : "1559561924550", "columnFamily" : "0", "row" : " 011114310", "type" : "Put", "value" : "[\'\\\\xE6\\\\xA1\\\\x82\\\\xE4\\\\xB8\\\\x9C\\\\xE5\\\\x8E\\\\xBF\']"}\n{"qualifier" : "TRANS_INDEX", "timestamp" : "1559561935152", "columnFamily" : "0", "row" : " 011114310", "type" : "Put", "value" : "0.5838"}')

   values=t[1].split("\n")


   # row={}
   first_row=json.loads(values[0])
   row = [first_row["columnFamily"], first_row["row"], first_row["timestamp"], first_row["type"]]
   for value in values:
       v=json.loads(value)


       # row["columnFamily"] = v["columnFamily"]
       # row["row"] = v["row"]
       # row["timestamp"] = v["timestamp"]
       # row["type"] = v["type"]

       row.append(bytes(v["value"], encoding="utf-8").decode(encoding="utf-8"))

   print(row)



