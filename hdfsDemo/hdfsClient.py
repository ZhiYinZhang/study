#-*- coding: utf-8 -*-
# @Time    : 2018/10/20 14:20
# @Author  : Z
# @Email   : S
# @File    : hdfsClient.py
from hdfs import client
import traceback as tb
from hdfs.ext.kerberos import KerberosClient
class hdfs_opt():
    def __init__(self,is_kerberos):
        self.is_kerberos=is_kerberos
    def __get_hdfs_client(self):
        # hdfs_host = "http://10.72.59.89:50070"
        # user = "entrobus"
        if self.is_kerberos:
            cli = KerberosClient(url=hdfs_host)
        else:
            cli = client.InsecureClient(url=hdfs_host, user=user)
        return cli
    def upload(self,hdfs_path,local_path,reties=3):
        cli=self.__get_hdfs_client()

        succ=""
        for i in range(reties):

           print(f"第{i+1}次")
           try:
                succ=cli.upload(hdfs_path,local_path,overwrite=True)
           except Exception:
                tb.print_exc()

           if len(succ) > 0:
               print("success")
               break
    def download(self,hdfs_path,local_path,overwrite=False):
        cli=self.__get_hdfs_client()
        result=cli.download(hdfs_path, local_path,overwrite=overwrite)
        if local_path in result:
            print("success")



def get_hdfs_client(is_kerberos=False):
    """

    :return: client of hdfs
    """
    # hdfs_host = "http://10.18.0.28:50070"
    # user="zhangzy"
    if is_kerberos:
        cli=KerberosClient(url=hdfs_host)
    else:
        cli = client.InsecureClient(url=hdfs_host,user=user)
        client.Client
    return cli


# hdfs_host="http://10.18.0.28:50070"
# user="zhangzy"

hdfs_host="http://10.72.59.89:50070"
user="entrobus"
import requests as rq
import sys
from requests_html import HTMLSession
from datetime import datetime as dt
if __name__=="__main__":
    cli=get_hdfs_client(is_kerberos=False)
    # for i in range(3):
    #     times = i + 1
    #     print(f"try {times} times")
    #     # 200
    #     response = rq.get("http://fdfs-test.entrobus.com/group1/M00/03/77/CmhqV11KfkuARO_wAACY6vX58d4575.csv")
    #     status_code = response.status_code
    #     if status_code == 200:
    #         print("success download")
    #         break
    #     if times == 3 and status_code != 200:
    #         print("failed download")
    #         sys.exit()
    # print("asdf")
    # #


    # start=dt.now()
    # cli.write(hdfs_path="./zhangzy/file1.csv",data=file_content,overwrite=True,encoding="utf-8",buffersize=1024*2)
    # print(cli.list("./"))
    # end=dt.now()
    # cli.download()



    succ=""
    for i in range(3):
       times=i+1
       print(f"第{times}次")

       try:
           #成功:succ为hdfs的目标路径
           tgt="/user/entrobus/external_data/"
           src="E:/test/tapd/人流-宜出行"

           # tgt = "/user/entrobus/zhangzy/dataset"
           # src = "E:\\test//cust_lng_lat//cust_addr//cust_lng_lat.csv"
           succ=cli.upload(tgt,src,overwrite=True)
       except Exception as e:
            tb.print_exc()
            if times==3:
                sys.exit()
       #成功 退出循环
       if len(succ)>0:
           print("success")
           break
