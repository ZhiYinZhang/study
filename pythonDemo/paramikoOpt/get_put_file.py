#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/1/2 9:03
import paramiko
import re
import time
import os
# host="10.72.59.92"
# user="entrobus"
# password="shangshang+2018"

host="10.18.0.34"
user="zhangzy"
password="zhangzy!@#$"

client=paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(host,22,user,password)

sftpClient:paramiko.SFTPClient=client.open_sftp()



#注意:在window使用sftpClient时，服务器路径分隔符要是/,这时使用os.path.join去拼接，分隔符为\\。不会报错，但是也不会成功

# cmd1="mkdir /home/entrobus/zhangzy/pkgs/paramiko"
# cmd2="cat /home/entrobus/zhangzy/pkgs/paramiko/log"
# cmd3="ls -l /home/entrobus/zhangzy/pkgs"
# client.exec_command(cmd1)

#拷贝远程目录下文件到本地目录下
# remotepath="/home/zhangzy/package/paramiko/"
# files=sftpClient.listdir(remotepath)
# print(files)
#
# localpath="E:/test/python模块依赖包/paramiko/"
# for file in files:
#     print(file)
#     remote_file_path=remotepath+file
#     local_file_path=localpath+file
#     sftpClient.get(remotepath=remote_file_path,localpath=local_file_path)


#拷贝本地目录下文件到远程目录下
remotepath="/home/zhangzy/package/paramiko/test/"
localpath="E:/test/python模块依赖包/paramiko/"

files=os.listdir(localpath)
for file in files:
    print(file)
    remote_file_path = remotepath + file
    local_file_path=localpath+file
    sftpClient.put(local_file_path,remote_file_path)


client.close()
