#-*- coding: utf-8 -*-
# @Time    : 2018/10/20 14:20
# @Author  : Z
# @Email   : S
# @File    : hdfsClient.py
from hdfs import client

def get_hdfs_client():
    """

    :return: client of hdfs
    """
    hdfs_host = "http://entrobus28:50070"
    user = "zhangzy"
    root = "/user"

    cli = client.InsecureClient(url=hdfs_host,user=user,root=root)
    return cli

