#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/11/27 15:31
from email.mime.text import MIMEText
import smtplib
from email.header import Header
"""
企业微信作为服务
"""
#输入email地址和口令
user = "张志颖"
from_addr = "zhangzy@entrobus.com"
password = "Zhangzhiying?35"


#smtp服务器地址
smtp_server = "smtp.exmail.qq.com"
to_addr = "2034690758@qq.com"

msg = MIMEText("1hello,send by python...","plain","utf-8")
msg['From'] = Header("菜鸟教程","utf-8")
msg['To'] = Header("测试","utf-8")

subject = "Python SMTP 邮件测试"
msg['Subject'] = Header(subject,'utf-8')


#使用ssl
# server = smtplib.SMTP_SSL()
# server.connect(smtp_server,465)

server = smtplib.SMTP()
server.connect(smtp_server,25)

server.login(from_addr,password)
server.sendmail(from_addr,[to_addr],msg.as_string())
