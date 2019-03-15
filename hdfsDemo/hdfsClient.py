#-*- coding: utf-8 -*-
# @Time    : 2018/10/20 14:20
# @Author  : Z
# @Email   : S
# @File    : hdfsClient.py
from hdfs import client
# from hdfs.ext.kerberos import KerberosClient
def get_hdfs_client():
    """

    :return: client of hdfs
    """
    hdfs_host = "http://entrobus28:50070"
    user = "zhangzy"
    root = "/user"

    cli = client.InsecureClient(url=hdfs_host,user=user,root=root)

    #kerberos 安全认证
    # cli = KerberosClient("http://entrobus28:50070")
    return cli


if __name__=="__main__":

  pass
