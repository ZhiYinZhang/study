#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/4/28 14:51
import phoenixdb
from phoenixdb import cursor
import pandas as pd
database_url="http://10.72.32.26:8765"
conn=phoenixdb.connect(database_url,max_retries=3,autocommit=True)

cursor=conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)

sql="select count(cust_id),classify_level1_code from tobacco.warning_code group by classify_level1_code"
sql1="select count(cust_id) from tobacco.retail where city='岳阳市' and (status='01' or status='02')"
sql2="select people_count from tobacco.retail where city='邵阳市'"

sql3="select avg_abno_time from tobacco.retail_warning limit 10"

sql4="select * from tobacco.warning_code limit 100"

sql5="select * from people_stream limit 100"



sql6="select avg_orders_plus4,avg_orders_minu4 from tobacco.warning_code where classify_level1_code='YJFL004'"
cursor.execute(sql6)
rows=cursor.fetchall()

# 68756
for row in rows:
    print(row)




