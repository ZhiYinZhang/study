#-*- coding: utf-8 -*-
# @Time    : 2019/1/6 11:28
# @Author  : Z
# @Email   : S
# @File    : expectDemo.py
import pexpect
import sys
import re
#用脚本实现自动登录，根据不同的返回，输入用户名和密码


# child = pexpect.spawn("ssh 10.18.0.29 'ls' ",logfile=sys.stdout,encoding='utf-8')
#
# try:
#     if child.expect([pexpect.TIMEOUT,'name',pexpect.EOF]):
#         child.sendline("123456")
#     if child.expect([pexpect.TIMEOUT,'password',pexpect.EOF]):
#         child.sendline("123456")
# except:
#     print(str(child))


from pexpect import pxssh

p=pxssh.pxssh()
p.login("10.18.0.34","zhangzy","zhangzy!@#$")
p.sendline("ifconfig")
p.prompt()