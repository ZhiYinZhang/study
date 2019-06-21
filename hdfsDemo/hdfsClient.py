#-*- coding: utf-8 -*-
# @Time    : 2018/10/20 14:20
# @Author  : Z
# @Email   : S
# @File    : hdfsClient.py
from hdfs import client
import traceback as tb
from hdfs.ext.kerberos import KerberosClient
def get_hdfs_client():
    """

    :return: client of hdfs
    """
    # hdfs_host = "http://entrobus28:50070"
    # user = "zhangzy"
    # root = "/user"
    hdfs_host = "http://10.72.59.90:50070"
    user="entrobus"
    root="/user"

    cli = client.InsecureClient(url=hdfs_host,user=user,root=root)

    #kerberos 安全认证
    cli = KerberosClient("http://entrobus28:50070")
    return cli


if __name__=="__main__":
    cli=get_hdfs_client()

    print(cli.list("zhangzy"))

    # while len(succ)==0:
    #    print(f"第{num}次")
    #    try:
    #         succ=cli.upload("/user/entrobus/tobacco_data_530/rent_food_hotel/","E:\资料\project\烟草\外部数据\租金餐饮酒店/hotel.csv",overwrite=True)
    #    except Exception as e:
    #         tb.print_exc()
    #    num+=1
    # print("success")

    # cli.makedirs("tobacco_data/gdp")
    # cli.delete("tobacco_data/gdp",True)

