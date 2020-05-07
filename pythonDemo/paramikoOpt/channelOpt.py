#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/1/1 14:12
import paramiko
import re
import time
"""
使用paramiko实现 自动输入 需要再次输入命令的操作
"""

host="10.18.0.34"
user="zhangzy"
password="zhangzy!@#$"

# 两种连接方式，paramiko.SSHClient()和paramiko.Transport()
# 第一种在cd后会回到连接的初始状态,如有两条命令 第一条是去到一个目录，第二条是在该目录下执行某些操作，
# 在执行完第一条之后，第二条执行时还是在初始的状态
def get_trans():
    trans=paramiko.Transport((host,22))
    # trans.connect(username=user,password=password)
    trans.start_client()
    trans.auth_password(username=user,password=password)
    return trans

trans=get_trans()
#开启一个通道
channel:paramiko.channel.Channel=trans.open_session()

#等待多久没有返回值，就退出
channel.settimeout(5)
#获取一个终端
channel.get_pty()
#激活器
channel.invoke_shell()



# channel.send("date>>/home/zhangzy/shell/log\r")
# channel.send("cd /home/zhangzy/shell\r")
#脚本里面涉及的路径最好写绝对路径，这个命令是在用户目录下执行的
channel.send("bash /home/zhangzy/shell/test.sh\r")
# channel.send("ls -l\r")


#在发送命令后，最好sleep一下，有的命令需要执行一些之间，不然可能没有结果或结果不完整
# time.sleep(0.5)
# rst = channel.recv(2048).decode("utf-8")
# print(rst)


p=re.compile("]\$ $")#以 ']$ '结尾
#遍历获取返回结果
while True:
  time.sleep(0.1)
  rst = channel.recv(2048).decode("utf-8")
  print(rst)
  if "yes/no" in rst:#是否包含yes/no
      channel.send("yes\r")
      time.sleep(1)
      rst = channel.recv(2048).decode("utf-8")
      print(rst)

  # 命令执行完后，会回到'[zhangzy@entrobus34 ~]$ '，是以"]& ",有的可能是"]# "
  #注意后面有一个空格
  if p.search(rst):
      break

channel.close()
trans.close()