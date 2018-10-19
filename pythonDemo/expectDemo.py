#!/usr/bin/env python
# -*- coding:utf-8 -*-
import pexpect

user='zhangzy'
password='zhangzy!@#$'
ip='10.18.0.26'
client=pexpect.spawn('ssh %s@%s'%(user,ip))
client.expect("*password:")
client.sendline(password)
client.sendline('echo "asdf">>/home/zhangzy/expect.txt')

