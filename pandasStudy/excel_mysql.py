#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/11 14:59
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime as dt
import traceback as tb
import os
from pandasStudy.properties import mysql_opt,hj_tables,hj_cols,qm_cols,qm_tables,monitor_path
# from pyinotify import WatchManager,Notifier,ProcessEvent,IN_DELETE,IN_CREATE,IN_MODIFY

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


user=mysql_opt["user"]
passwd=mysql_opt["passwd"]
host=mysql_opt["host"]
port=mysql_opt["port"]
db_name=mysql_opt["db_name"]
engine=create_engine(f'mysql+pymysql://{user}:{passwd}@{host}:{port}/{db_name}')


def excel_to_mysql(file,tables,cols):
    print(file)
    #excel 表
    excel_tables=list(tables.keys())

    #获取数据更新时间
    update_time = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    # update_time="2019-04-28 16:34:59"
    print(f"update time {update_time}")

    for excel_table in excel_tables:
        try:
            print(f"read {excel_table}")
            df = pd.read_excel(io=file, sheet_name=excel_table)
            #插入更新时间
            df["更新时间"] = update_time


            #修改列名
            renames = cols[excel_table]
            df: pd.DataFrame = df.rename_axis(mapper=renames, axis=1)

            df = df.replace("\s+(-)\s+", 0,regex=True)

            sql_table = tables[excel_table]
            print(f"write {sql_table}")
            # print(df)
            df.to_sql(name=sql_table,con=engine,index=False,if_exists="append")
        except Exception as e:
            tb.print_exc()
# def get_new_file(dir):
#     # 获取创建时间最新的文件
#     files = os.listdir(dir)
#     tuples = []
#     for file in files:
#         path = dir + file
#         t = (path, os.path.getctime(path))
#         tuples.append(t)
#
#     tuples = sorted(tuples, key=lambda x: x[1], reverse=True)
#
#     target_file = tuples[0][0]
#     return target_file
# def qm_to_mysql(file,tables,cols):
#     excel_tables = list(tables.keys())
#     update_time= str(dt.now().date())
#
#     for excel_table in excel_tables:
#         df = pd.read_excel(io=file, sheet_name=excel_table)
#         df = df.rename_axis(mapper=cols[excel_table], axis=1)
#
#         df["update_time"]=update_time
#         df["company"] = "千摩"
#         df["time"] = "2019-" + df["time"]
#         df["time"] = df["time"].replace(to_replace="月", value="", regex=True)
#
#         sql_table = tables[excel_table]
#         df.to_sql(name=sql_table, con=engine, index=False, if_exists="append")
#         print(df)


# class EventHandler(ProcessEvent):
#     def process_IN_CREATE(self, event):
#         print("create:", event.path, event.name)
#         path = event.path + "/" + event.name
#         excel_to_mysql(path, hj_tables, hj_cols)
#
#     def process_IN_DELETE(self, event):
#         print("delete:", event.path, event.name)
#
# def fsMonitor(path="."):
#     wm = WatchManager()  # 创建监控组
#     mask = IN_CREATE | IN_DELETE
#     wm.add_watch(path, mask, auto_add=True, rec=True)# 将具体路径的监控加入监视组
#     notifier = Notifier(wm, EventHandler())# 创建事件处理器，参数为监视组和对应的事件处理函数
#     print("now starting monitor %s." % path)
#     while True:
#         try:
#             notifier.process_events()  # 对事件队列中的事件逐个调用事件处理函数
#             if notifier.check_events():  # 等待 检查是否有新事件到来
#                 print("check event true")
#                 notifier.read_events()  # 将新事件读入事件队列
#         except KeyboardInterrupt:
#             print("keyboard interrupt")
#             notifier.stop()
if __name__=="__main__":
    # fsMonitor(monitor_path)
    excel_to_mysql(file="E:\\test\ouhao\hj\昊居数据库数据导入0522.xlsx",tables=hj_tables,cols=hj_cols)