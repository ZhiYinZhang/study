#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/12/31 12:00
import paramiko

def get_tran1():

    # 获取Transport实例
    tran = paramiko.Transport((host, port))

    # 连接SSH服务端，使用password
    tran.connect(username=user, password=password)


    #也可以通过SSHClient获取Transport实例
    # client=paramiko.SSHClient()
    # client.set_missing_host_key_policy(paramiko.AutoAddPolicy)
    # client.connect(host,user,password)
    # tran=client.get_transport()

    return tran
def get_tran2():
    # 获取Transport实例
    tran = paramiko.Transport(('10.18.0.34', 22))

    # 配置私人密钥文件位置
    private = paramiko.RSAKey.from_private_key_file('/Users/root/.ssh/id_rsa')
    # 连接SSH服务端，使用pkey指定私钥
    tran.connect(username="zhangzy", pkey=private)

    return tran



host="10.18.0.34"
port=22
user="zhangzy"
password="zhangzy!@#$"

tran=get_tran1()

# 通过Transport获取SFTP实例
sftp = paramiko.SFTPClient.from_transport(tran)

#还可以直接通过SSHClient.open_sftp()获取SFTPClient对象

# 设置上传的本地/远程文件路径
localpath = "E:\chromeDownload\itemId1.csv"
remotepath = "/home/zhangzy/tobacco_data/itemId.csv"

# 执行上传动作
# sftp.put(localpath, remotepath)
# 执行下载动作
sftp.get(remotepath, localpath)

tran.close()