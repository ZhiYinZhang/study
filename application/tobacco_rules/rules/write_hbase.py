#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/4/8 16:35
import pandas as pd
import numpy as np
import traceback as tb
import subprocess as sp
import happybase
from happybase import Table
import uuid
from rules.config import hbase_pool_size,hbase_host,phoenix_home


def write_hbase(rows,hbase):
    """
    spark row 的不同列写入不同family column
    :param rows:   list[Row]
    :param hbase
             hbase = {"table": "test","row": "id", "families": ["fly_col1","fly_col2"],
                  "fly_col1":["col1","col2"],"fly_col2":["col3","col4"]}
          row  DataFrame的那列作为hbase的row key
          families hbase表的列族，
          "fly_col1" 为fly_col1列族需要DataFrame的那些列   DataFrame的列名即为hbase的列名
    """
    pool = happybase.ConnectionPool(host=hbase_host, size=hbase_pool_size)
    with pool.connection() as conn:
        table: Table = conn.table(hbase["table"])
        try:
            with table.batch(batch_size=1000) as batch:
                for row in rows:
                    row_key = str(row[hbase["row"]])  # row key
                    data = {}
                    for family in hbase["families"]:  # 列族
                        for col in hbase[family]:    #每个列族 的列
                             fly_col=f"{family}:{col}"
                             data[fly_col]=str(row[col])
                    # data={"family1:col1":value1,"family1:col2":value2}
                    batch.put(row=row_key,data=data)
        except Exception as e:
            print(e.args)





def write_hbase1(rows,cols,hbase):
    """
      spark row中的cols包含的列 写入同一个family column
    :param rows:   list[Row]
    :param cols:   list[colName] 需要写入hbase的DataFrame的列 并且与hbase的列名对应
    :param hbase: {"table":hbase_tableName,"row":row_key,"families":[fly_col1]}
                   table  hbase中的表
                   row    DataFrame中为row key的列
                   families hbase中的列族
    """
    pool = happybase.ConnectionPool(host=hbase_host, size=hbase_pool_size)
    with pool.connection() as conn:
        table: Table = conn.table(hbase["table"])
        try:
            with table.batch(batch_size=1000) as batch:
                for row in rows:
                    data = {}
                    row_key = str(row[hbase["row"]])  # row key
                    for family in hbase["families"]:  # 列族
                        for c in cols:  # 列
                            fly_col = f"{family}:{c.upper()}"
                            data[fly_col] = str(row[c])
                        # data={"family1:col1":value1,"family1:col2":value2}
                        batch.put(row=row_key, data=data)
        except Exception as e:
            print(e.args)
def write_hbase2(rows,cols,hbase):
    """
      spark row中的cols包含的列 写入同一个family column
    :param rows:   list[Row]
    :param cols:   list[colName] 需要写入hbase的DataFrame的列 并且与hbase的列名对应
    :param hbase: {"table":hbase_tableName,"row":row_key,"families":[fly_col1]}
                   table  hbase中的表
                   families hbase中的列族
    """
    pool = happybase.ConnectionPool(host=hbase_host, size=hbase_pool_size)
    with pool.connection() as conn:
        table: Table = conn.table(hbase["table"])
        try:
            with table.batch(batch_size=1000) as batch:
                for row in rows:
                    data = {}
                    row_key=str(uuid.uuid1()).replace("-","")
                    for family in hbase["families"]:  # 列族
                        for c in cols:  # 列
                            fly_col = f"{family}:{c.upper()}"
                            data[fly_col] = str(row[c])
                        # data={"family1:col1":value1,"family1:col2":value2}
                        batch.put(row=row_key, data=data)
        except Exception as e:
            print(e.args)


def pd_write_hbase(df:pd.DataFrame,cols:list,param,upper=False):
        """
        :param df:  pandas DataFrame
        :param cols: 除row_key之外
        """
        host = param["host"]
        table = param["table"]
        row = param["row"]
        family = param["family"]

        pool=happybase.ConnectionPool(host=host,size=10)
        with pool.connection() as conn:
            table: Table = conn.table(table)
            try:
                with table.batch(batch_size=1000) as batch:
                    for x in range(len(df)):
                        l = df.iloc[x][cols].values

                        row_key=df.iloc[x][row] # row key

                        data = {}
                        for y in range(len(cols)):
                            if upper:
                                c=cols[y].upper()
                            else:
                                c=cols[y]

                            fly_col = f"{family}:{c}"
                            data[fly_col] = str(l[y])
                        batch.put(row=str(row_key), data=data)
            except Exception as e:
                print(e.args)


def delete_all(table_name,columns=None,row_prefix=None):
    """

    :param table_name: 表名
    :param column:  注意大小写  只是为了scan返回数据少一点
    """
    pool = happybase.ConnectionPool(host=hbase_host, size=hbase_pool_size)
    with pool.connection() as conn:
        table = conn.table(table_name)

        print(table_name)

        if columns:
            for i in range(len(columns)):
                     columns[i]=f"0:{columns[i]}"

        rows = table.scan(columns=columns, row_prefix=bytes(row_prefix, "utf-8"))

        size=0
        with table.batch(batch_size=10000) as batch:
            for row in rows:
                size+=1
                # print(row[0],row[1])
                batch.delete(row[0])
        print("size:",size)

def create_table(table_name):
    conn=happybase.Connection(host=hbase_host)
    conn.create_table(name=table_name, families={"0": {}})


def create_ph_table(table_name):
    """
    创建对应表的临时表
    :param table_name:table_name 为要创建临时表的表，如创建test表的临时表test_temp
    """

    temp_table=table_name+"_TEMP"



    #最后的EOF前面不能有空格
    #档位投放表
    gears_toss=f"""
exec python2 {phoenix_home}/bin/sqlline.py <<EOF

CREATE TABLE {temp_table}(
"ROW" VARCHAR PRIMARY KEY,cust_id VARCHAR,city VARCHAR, county VARCHAR,
sale_center_id VARCHAR, cust_name VARCHAR,longitude VARCHAR,latitude VARCHAR,
gears VARCHAR, gauge_id VARCHAR,gauge_name VARCHAR,brand_id VARCHAR,brand_name VARCHAR, 
reserve_min_ciga5_name VARCHAR,gauge_week_planned_volume VARCHAR, gauge_week_reality_volume VARCHAR,
gauge_week_residue_volume VARCHAR,gauge_week_reserve_ratio VARCHAR,gauge_week_reserve_face VARCHAR, 
gauge_week_stock_face VARCHAR,gauge_week_again VARCHAR,county_gauge_reserve VARCHAR,
retail_gauge_week_reserve VARCHAR, county_gauge_week_volume VARCHAR,county_gears_score VARCHAR,
county_gears_volume_num VARCHAR,gauge_advise_volume VARCHAR, gears_data_marker VARCHAR
) column_encoded_bytes=0;

EOF
    """

    #卷烟画像表
    cigar_picture=f"""
exec python2 {phoenix_home}/bin/sqlline.py <<EOF

CREATE TABLE {temp_table}(
"ROW" varchar PRIMARY KEY,CITY varchar,COUNTY varchar,SALE_CENTER_ID varchar,
BRAND_ID varchar,BRAND_NAME varchar,GAUGE_ID varchar,GAUGE_NAME varchar,
BRAND_CITY_ORDERS varchar,BRAND_COUNTY_ORDERS varchar,BRAND_CITY_ORDER_AMOUNT varchar,BRAND_COUNTY_ORDER_AMOUNT varchar,
BRAND_CITY_MONTH_SALES varchar,BRAND_CITY_WEEK_SALES varchar,BRAND_CITY_MONTH_ORDERS varchar,BRAND_CITY_MONTH_SALES_RATIO varchar,
BRAND_CITY_MONTH_SALES_LAST_YEAR varchar,BRAND_CITY_WEEK_SALES_LAST_YEAR varchar,BRAND_CITY_MONTH_ORDERS_LAST_YEAR varchar,BRAND_CITY_MONTH_RETIO_LAST_YEAR varchar,
BRAND_CITY_MONTH2_SALES varchar,BRAND_CITY_WEEK2_SALES varchar,BRAND_CITY_MONTH2_ORDERS varchar,BRAND_CITY_MONTH2_SALES_RATIO varchar,
BRAND_COUNTY_MONTH_SALES varchar,BRAND_COUNTY_WEEK_SALES varchar,BRAND_COUNTY_MONTH_ORDERS varchar,BRAND_COUNTY_MONTH_SALES_RATIO varchar,
BRAND_COUNTY_MONTH_SALES_LAST_YEAR varchar,BRAND_COUNTY_WEEK_SALES_LAST_YEAR varchar,BRAND_COUNTY_MONTH_ORDERS_LAST_YEAR varchar,BRAND_COUNTY_MONTH_RETIO_LAST_YEAR varchar,
BRAND_COUNTY_MONTH2_SALES varchar,BRAND_COUNTY_WEEK2_SALES varchar,BRAND_COUNTY_MONTH2_ORDERS varchar,BRAND_COUNTY_MONTH2_SALES_RATIO varchar,
BRAND_CITY_GAUGE_SALES_RATIO varchar,BRAND_COUNTY_GAUGE_SALES_RATIO varchar,GAUGE_CITY_ORDERS varchar,GAUGE_COUNTY_ORDERS varchar,
GAUGE_CITY_ORDER_AMOUNT varchar,GAUGE_COUNTY_ORDER_AMOUNT varchar,GAUGE_CITY_MONTH_SALES varchar,GAUGE_CITY_WEEK_SALES varchar,
GAUGE_CITY_MONTH_ORDERS varchar,GAUGE_CITY_MONTH_SALES_RATIO varchar,GAUGE_CITY_MONTH_SALES_LAST_YEAR varchar,
GAUGE_CITY_WEEK_SALES_LAST_YEAR varchar,GAUGE_CITY_MONTH_ORDERS_LAST_YEAR varchar,GAUGE_CITY_MONTH_RETIO_LAST_YEAR varchar,
GAUGE_CITY_MONTH2_SALES varchar,GAUGE_CITY_WEEK2_SALES varchar,GAUGE_CITY_MONTH2_ORDERS varchar,GAUGE_CITY_MONTH2_SALES_RATIO varchar,
GAUGE_COUNTY_MONTH_SALES varchar,GAUGE_COUNTY_WEEK_SALES varchar,GAUGE_COUNTY_MONTH_ORDERS varchar,
GAUGE_COUNTY_MONTH_SALES_RATIO varchar,GAUGE_COUNTY_MONTH_SALES_LAST_YEAR varchar,GAUGE_COUNTY_WEEK_SALES_LAST_YEAR varchar,
GAUGE_COUNTY_MONTH_ORDERS_LAST_YEAR varchar,GAUGE_COUNTY_MONTH_RETIO_LAST_YEAR varchar,GAUGE_COUNTY_MONTH2_SALES varchar,
GAUGE_COUNTY_WEEK2_SALES varchar,GAUGE_COUNTY_MONTH2_ORDERS varchar,GAUGE_COUNTY_MONTH2_SALES_RATIO varchar,GAUGE_CITY_RETAIL_RATIO varchar,
GAUGE_COUNTY_RETAIL_RATIO varchar,GAUGE_PROP_LIKE_CIGA varchar,GAUGE_CLIENT_LIKE_CIGA varchar,GAUGE_CITY_SALES_HISTORY varchar,
GAUGE_COUNTY_SALES_HISTORY varchar,GAUGE_SALES_FORECAST varchar,CIGA_DATA_MARKER varchar
) column_encoded_bytes=0;

EOF
    """

    #卷烟评分表
    cigar_grade=f"""
exec python2 {phoenix_home}/bin/sqlline.py <<EOF

CREATE TABLE {temp_table}(
"ROW" varchar PRIMARY KEY,
CUST_ID varchar,
CITY varchar,
COUNTY varchar,
SALE_CENTER_ID varchar,
CUST_NAME varchar,
LONGITUDE varchar,
LATITUDE varchar,
BRAND_ID varchar,
BRAND_NAME varchar,
BRAND_GRADE varchar,
GAUGE_GRADE varchar,
GAUGE_ID varchar,
GAUGE_NAME varchar,
GRADE_DATA_MARKER varchar
)column_encoded_bytes=0;

EOF
    """


    suffix=table_name.split(".")[1]
    if suffix=="GEARS_TOSS":
        command=gears_toss
    elif suffix=="CIGA_PICTURE":
        command=cigar_picture
    elif suffix=="CIGA_GRADE":
        command=cigar_grade

    sp.check_output(command, shell=True)

    print(f"创建临时表{temp_table}成功")


def switch_table(table_name):
      """
       table_name保存的过去的数据，
       table_name的临时表保存的是最新的数据
       1.先删除掉table_name
       2.将table_name的临时表重命名为table_name
       3.删除掉table_name的临时表
      :param table_name:
      """
      temp_table=table_name+"_TEMP"

      command=f"""
temp_table={temp_table}
table={table_name} 

exec hbase shell <<EOF

truncate "$table"
disable "$table"
drop "$table"

disable "$temp_table"
snapshot "$temp_table","temp_snapshot"
clone_snapshot "temp_snapshot","$table"
delete_snapshot "temp_snapshot"
drop "$temp_table"

EOF
      """

      print(f"将{temp_table}表重命名为{table_name}")
      sp.check_output(command,shell=True)
      print("重命名成功")

def rebuild_index(table_name,index_table_name):
    """

    :param table_name: 需要重新建立索引的表
    :param index_table_name:  对应的索引表
    """
    command = f"""
exec python2 {phoenix_home}/bin/sqlline.py <<EOF

alter index {index_table_name} on {table_name} REBUILD;

EOF
        """
    print(f"rebuild {table_name}表的索引")
    sp.check_output(command, shell=True)
    print("rebuild success")


if __name__=="__main__":
    # delete_all("TOBACCO.RETAIL_WARNING","CITY")
    pass
    # df=pd.read_csv("E:/test_return(1).csv")
    # print(df)
    # pd_write_hbase(df,["sarima_weightd_overall_alarming","historical_overall_alarming"])
