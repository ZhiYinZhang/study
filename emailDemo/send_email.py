#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/11/27 15:31
from email.mime.text import MIMEText
import smtplib
from email.header import Header

#输入email地址和口令
user = "张志颖"
from_addr = "zhangzy@entrobus.com"
password = "Zhangzhiying?35"

#smtp服务器地址
smtp_server = "smtp.exmail.qq.com"
to_addr = "zhangzy@entrobus.com"

msg = MIMEText("hello,send by python...","plain","utf-8")
msg['From'] = Header("菜鸟教程","utf-8")
msg['To'] = Header("测试","utf-8")

subject = "Python SMTP 邮件测试"
msg['Subject'] = Header(subject,'utf-8')

try:
    server = smtplib.SMTP()
    server.connect(smtp_server,25)

    server.login(from_addr,password)
    server.sendmail(from_addr,[to_addr],msg.as_string())
    print("邮件发送成功")
except smtplib.SMTPException:
    print("无法发送邮件")