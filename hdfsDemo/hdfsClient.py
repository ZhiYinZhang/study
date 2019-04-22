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
    # hdfs_host = "http://entrobus28:50070"
    # user = "zhangzy"
    # root = "/user"
    hdfs_host = "http://10.72.59.90:50070"
    user="entrobus"
    root="/user/entrobus"

    cli = client.InsecureClient(url=hdfs_host,user=user,root=root)

    #kerberos 安全认证
    # cli = KerberosClient("http://entrobus28:50070")
    return cli


if __name__=="__main__":
    cli=get_hdfs_client()
    # cli.upload("zhangzy/population","E:\\test\邵阳岳阳株洲人口数据\邵阳岳阳株洲人口数据",True)
