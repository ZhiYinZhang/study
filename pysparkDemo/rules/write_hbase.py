#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/4/8 16:35

import happybase
from happybase import Table

hbase = {"host": "10.72.32.26", "size": 10, "table": "member3",
         "row": "cust_id", "familys": ["column_A"]}

def write_hbase1(rows, cols):
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
                        batch.put(row=row_key, data=data)
        except Exception as e:
            print(e.args)