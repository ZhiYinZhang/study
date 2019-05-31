#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/5/15 14:01
import phoenixdb
from phoenixdb import cursor
import pandas as pd

def pandas_read_phoenix(cursor,sql_statement:str,batch=10):
      cursor.execute(sql_statement)

      rows=cursor.fetchall()


      cols=list(rows[0].keys())
      df=pd.DataFrame(columns=cols)
      df_rows=[]


      for row in rows:

          df_rows.append(row)
          if len(df_rows)==batch:
              df=df.append(df_rows)
              df_rows.clear()
      return df


if __name__=="__main__":
    database_url = "http://10.72.32.26:8765"
    conn = phoenixdb.connect(database_url, max_retries=3, autocommit=True)
    cursor = conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)

    sql="select count(distinct(cust_id)),classify_level1_code from tobacco.warning_code group by classify_level1_code"


    df=pandas_read_phoenix(cursor, sql)


    print(df)