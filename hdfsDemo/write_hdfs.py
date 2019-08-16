#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/7/25 18:36
from hdfs import client
import traceback as tb
from datetime import datetime as dt
import os

def get_hdfs_client():
    """
    :return: client of hdfs
    """
    hdfs_host = "http://10.72.59.89:50070"
    user = "entrobus"

    cli = client.InsecureClient(url=hdfs_host, user=user)
    return cli


def log(data, is_faild=False):
    cli = get_hdfs_client()

    dirs = "/user/entrobus/zhangzy/dataset/tobacco_log"

    timestamp = dt.now()
    file_name = str(timestamp.date()) + ".log"
    path = os.path.join(dirs, file_name)

    if is_faild:
        level = "success"
    else:
        level = "faild"

    data = f"{timestamp}\t[ {level} ]\t{data} \n".encode("utf-8")
    if not cli.status(path, strict=False):
        # 创建
        cli.write(hdfs_path=path, data='', overwrite=True)
    cli.write(hdfs_path=path, data=data, append=True)