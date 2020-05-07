#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/1/1 14:12
import paramiko
from datetime import datetime as dt
import re
import time
import os
# host="10.72.59.92"
# user="entrobus"
# password="shangshang+2018"

user="root"
password="ycbigd@636"

# host="10.18.0.34"
# user="zhangzy"
# password="zhangzy!@#$"
hosts=["10.72.59.89","10.72.59.90","10.72.59.91","10.72.59.92"]

for host in hosts:
    print(f"{str(dt.now())} info host:{host}")
    client=paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host,22,user,password)

    remotepath="/home/entrobus/zhangzy/pkgs/paramiko/"
    stdin,stdout,stderr=client.exec_command(f"pip install  --no-index --find-links={remotepath} paramiko")
    rst=stdout.read().decode("utf-8")
    rst_err=stderr.read().decode("utf-8")
    print(rst)
    print(rst_err)

    # sftpClient:paramiko.SFTPClient=client.open_sftp()
    #
    # remotepath="/home/entrobus/zhangzy/pkgs/paramiko/"
    # try:
    #  sftpClient.mkdir(remotepath)
    # except:
    #     print(f"{str(dt.now())} warn directory already exists")
    #
    #
    #
    # localpath="E:/test/python模块依赖包/paramiko/"
    #
    # files=os.listdir(localpath)
    # for file in files:
    #     print(f"{str(dt.now())} info file:{file}")
    #     remote_file_path = os.path.join(remotepath,file)
    #     local_file_path=os.path.join(localpath,file)
    #
    #     sftpClient.put(local_file_path,remote_file_path)


    client.close()
