#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/12/6 17:37

import paramiko

client=paramiko.SSHClient()

client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

client.connect(hostname="10.18.0.34",port=22,username="zhangzy",password="zhangzy!@#$")

stdin,stdout,stderr=client.exec_command("hdfs dfs -ls /")


print(stdout.read().decode("utf-8"))


client.close()