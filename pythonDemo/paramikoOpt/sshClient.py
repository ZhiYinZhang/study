#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/12/31 11:35
"""
paramiko包含两个核心组件：SSHClient和SFTPClient。
    SSHClient的作用类似于Linux的ssh命令，是对SSH会话的封装，该类封装了传输(Transport)
                    ，通道(Channel)及SFTPClient建立的方法(open_sftp)，通常用于执行远程命令。
    SFTPClient的作用类似与Linux的sftp命令，是对SFTP客户端的封装，用以实现远程文件操作，如文件上传、下载、修改文件权限等操作。

# Paramiko中的几个基础名词：
1、Channel：是一种类Socket，一种安全的SSH传输通道；
2、Transport：是一种加密的会话，使用时会同步创建了一个加密的Tunnels(通道)，这个Tunnels叫做Channel；
3、Session：是client与Server保持连接的对象，用connect()/start_client()/start_server()开始会话。
"""
import paramiko
def get_client1():
    #使用密码连接方式

    client = paramiko.SSHClient()

    # 自动添加策略，保存服务器的主机名和密钥信息，如果不添加，那么不再本地know_hosts文件中记录的主机将无法连接
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # 连接ssh服务端，以用户名和密码进行认证
    client.connect(hostname="10.18.0.34", port=22, username="zhangzy", password="zhangzy!@#$")
    return client

def get_client2():
    #使用密钥连接

    # 配置本地私人密钥文件位置
    private = paramiko.RSAKey.from_private_key_file('/Users/ch/.ssh/id_rsa')

    # 实例化SSHClient
    client = paramiko.SSHClient()

    # 自动添加策略，保存服务器的主机名和密钥信息，如果不添加，那么不再本地know_hosts文件中记录的主机将无法连接
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # 连接SSH服务端，以用户名和密码进行认证
    client.connect(hostname='10.18.0.34', port=22, username='zhangzy', pkey=private)
    return client

if __name__=="__main__":
    client:paramiko.client.SSHClient=get_client1()

    # cmd="date>>/home/zhangzy/tobacco_data/ssh.csv"
    cmd="bash /home/zhangzy/shell/test.sh"
    stdin,stdout,stderr=client.exec_command(cmd)
    # stdin:paramiko.channel.ChannelStdinFile=stdin
    print(stdout.read().decode("utf-8"))


    #关闭sshClient
    client.close()