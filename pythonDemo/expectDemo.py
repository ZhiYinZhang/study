#-*- coding: utf-8 -*-
# @Time    : 2019/1/6 11:28
# @Author  : Z
# @Email   : S
# @File    : expectDemo.py
import pexpect
import sys
import re


child = pexpect.spawn("ssh 10.18.0.29 'ls' ",logfile=sys.stdout,encoding='utf-8')

try:
    if child.expect([pexpect.TIMEOUT,'name']):
        child.sendline("123456")
    if child.expect([pexpect.TIMEOUT,'password']):
        child.sendline("123456")
except:
    print(str(child))

try:
    child.expect([pexpect.TIMEOUT,pexpect.EOF])


except:
    print(str(child))

