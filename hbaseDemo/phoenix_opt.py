#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/4/28 14:51
import phoenixdb
from phoenixdb import cursor
import pandas as pd
database_url="http://10.72.32.26:8765"
conn=phoenixdb.connect(database_url,max_retries=3,autocommit=True)

cursor=conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)

sql="select * from tobacco.code limit 100"
cursor.execute(sql)
rows=cursor.fetchall()


for row in rows:
    print(row)




