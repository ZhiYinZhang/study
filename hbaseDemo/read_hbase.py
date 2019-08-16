#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import happybase
from happybase import Table

hbase={"host":"10.72.59.89","size":10,"table":"V630_TOBACCO.GEARS_TOSS",
       "row":"cust_id",
       "families":["0"]}

conn=happybase.Connection(hbase["host"])
table=conn.table(hbase["table"])

table1=conn.table("test2")

rows=table.scan()

size=0
for row in rows:
       size+=1
       table1.put(row=row[0],data=row[1])
print(size)