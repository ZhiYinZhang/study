#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import happybase
from happybase import Table

hbase={"host":"10.18.0.34","size":10,"table":"test2",
       "row":"cust_id",
       "families":["0"]}

conn=happybase.Connection(hbase["host"])
table=conn.table(hbase["table"])

conn.create_table(name="temp",families={"0":{}})
