#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/10/28 10:55
from hdfsDemo.hdfsClient import get_hdfs_client
import os
"""
获取烟草 所有hive表 分区数据为0的分区 
"""
cli=get_hdfs_client()

#hive外部表数据存储位置
root_dir="/user/aistrong/data/admin/data/db2"

#获取所有表
tables = cli.list(root_dir)

#遍历
for table in tables:
    print(f"|--table {table}")
    #表的路径
    # table_path = os.path.join(root_dir, table)
    table_path = "/".join([root_dir, table])

    #获取表的所有分区
    partitions = cli.list(table_path)
    for part in partitions:
        #分区路径
        # partition_path = os.path.join(table_path, part)
        partition_path = "/".join([table_path, part])

        #获取分区下面所有文件  <=1
        files = cli.list(partition_path)

        if len(files) > 0:
            #文件路径
            # file_path = os.path.join(partition_path, files[0])
            file_path = "/".join([partition_path, files[0]])

            #获取文件信息
            status = cli.status(file_path)
            #通过status["length"]获取文件大小
            size = round(status["length"] / 1024 / 1024, 3)
            if status["length"] == 0:
                print(f"\t|--partition {part} size: {size}M")
        else:
            print(f"\t|--partition {part} size: 0M")