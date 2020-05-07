#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/6/26 17:27
import paramiko
from datetime import datetime as dt

def getSHHClient():
   sshClient=paramiko.SSHClient()
   sshClient.set_missing_host_key_policy(paramiko.AutoAddPolicy())
   sshClient.connect(hostname="10.18.0.34", port=22, username="zhangzy", password="zhangzy!@#$")
   return sshClient

sshClient=getSHHClient()


cmd="sudo jps|grep ThriftServer"
stdin,stdout,stderr=sshClient.exec_command(cmd)
out=stdout.read().decode("utf-8")
if "ThriftServer" in out:
    print(f"{str(dt.now())} {out}")
else:
    cmd="sudo /opt/hbase-1.2.6/bin/hbase-daemon.sh restart thrift"
    print(f"""
    hbase thrift 意外停止，准备重启thrift
    执行命令:{cmd}
    """)
    stdin,stdout,stderr=sshClient.exec_command(cmd)
    print(stdout.read().decode("utf-8"))

sshClient.close()
