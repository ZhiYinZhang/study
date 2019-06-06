#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/4/28 14:51
import phoenixdb
from phoenixdb import cursor
database_url="http://10.72.59.91:8765/"
conn=phoenixdb.connect(database_url,autocommit=True)

cursor=conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)

sql="select count(cust_id),classify_level1_code from tobacco.warning_code group by classify_level1_code"
sql1="select count(cust_id) from tobacco.retail where city='岳阳市' and (status='01' or status='02')"
sql2="select people_count from tobacco.retail where city='邵阳市'"

sql3="select avg_abno_time from tobacco.retail_warning limit 10"

sql4="select * from tobacco.warning_code limit 100"

sql5="select * from people_stream limit 100"


cursor.execute("select * from tobacco.retail limit 10")
rows=cursor.fetchall()

# 68756
for row in rows:
    print(row)




