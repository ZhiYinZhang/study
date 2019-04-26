#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/11 14:59
from sqlalchemy import create_engine
import pandas as pd
import pymysql
from datetime import datetime as dt
import traceback as tb
import os
from pandasStudy.properties import mysql_opt,hj_tables,hj_cols,qm_cols,qm_tables
# user="root"
# passwd="ouhao#18"
# host="120.78.127.137"
# port='3306'
# db_name="ouhaodw"
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


user=mysql_opt["user"]
passwd=mysql_opt["passwd"]
host=mysql_opt["host"]
port=mysql_opt["port"]
db_name=mysql_opt["db_name"]
engine=create_engine(f'mysql+pymysql://{user}:{passwd}@{host}:{port}/{db_name}')


dir="E:\\test\\ouhao\\hj\\"
# hj_path=dir+"昊居数据库字段(6).xlsx"
# qm_path=dir+"千摩销售目标及回款2019(1).xlsx"


def excel_to_mysql(file,tables,cols):
    print(file)
    #excel 表
    excel_tables=list(tables.keys())

    update_time = str(dt.now().date())

    for excel_table in excel_tables:
        try:
            # excel_table = "昊居签约数据"

            print(f"read {excel_table}")
            df = pd.read_excel(io=file, sheet_name=excel_table)
            #插入更新时间
            df["更新时间"] = update_time

            # 修改列名
            renames = cols[excel_table]
            df: pd.DataFrame = df.rename_axis(mapper=renames, axis=1)

            # print(df.dtypes)

            if excel_table in ["昊居应收账款","昊居区域利润"]:
                print(1)
                df = df.replace("\s+(-)\s+", 0,regex=True)
            else:
                df = df.replace("\s+(-)\s+", '',regex=True)
                print(0)

            sql_table = tables[excel_table]
            print(f"write {sql_table}")

            # print(df["refund_reason"])
            # print(df)
            df.to_sql(name=sql_table,con=engine,index=False,if_exists="append")
        except Exception as e:
            tb.print_exc()

def qm_to_mysql(file,tables,cols):
    excel_tables = list(tables.keys())
    update_time= str(dt.now().date())

    for excel_table in excel_tables:
        df = pd.read_excel(io=file, sheet_name=excel_table)
        df = df.rename_axis(mapper=cols[excel_table], axis=1)

        df["update_time"]=update_time
        df["company"] = "千摩"
        df["time"] = "2019-" + df["time"]
        df["time"] = df["time"].replace(to_replace="月", value="", regex=True)

        sql_table = tables[excel_table]
        df.to_sql(name=sql_table, con=engine, index=False, if_exists="append")
        print(df)
if __name__=="__main__":
    dir = "E:\\test\\ouhao\\hj\\"
    # dir = "E:\\test\\ouhao\\qm\\"
    files = os.listdir(dir)
    tuples = []
    for file in files:
        path = dir + file
        t = (path, os.path.getctime(path))
        tuples.append(t)

    tuples = sorted(tuples, key=lambda x: x[1], reverse=True)

    # print(tuples)
    target_file=tuples[0][0]


    excel_to_mysql(target_file,hj_tables,hj_cols)
    # qm_to_mysql(qm_path,qm_tables,qm_cols)
