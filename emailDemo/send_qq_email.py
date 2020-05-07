#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/12/18 17:26
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr

my_sender = '2034690758@qq.com'  # 发件人邮箱账号
my_pass = 'qpyfcdlwhxzpeafd'  # 发件人邮箱密码,授权码'
my_user = '2034690758@qq.com'  # 收件人邮箱账号，我这边发送给自己



msg = MIMEText('linux填写邮件内容', 'plain', 'utf-8')
msg['From'] = formataddr(["憧憬...", my_sender])  # 括号里的对应发件人邮箱昵称、发件人邮箱账号
msg['To'] = formataddr(["憧憬...", my_user])  # 括号里的对应收件人邮箱昵称、收件人邮箱账号
msg['Subject'] = "菜鸟教程发送邮件测试"  # 邮件的主题，也可以说是标题

server = smtplib.SMTP("smtp.qq.com", 25)  # 发件人邮箱中的SMTP服务器
# server = smtplib.SMTP_SSL("smtp.qq.com", 465)  # 发件人邮箱中的SMTP服务器,使用ssl加密，端口是465

server.login(my_sender, my_pass)  # 括号中对应的是发件人邮箱账号、邮箱密码
server.sendmail(my_sender, [my_user, ], msg.as_string())  # 括号中对应的是发件人邮箱账号、收件人邮箱账号、发送邮件
server.quit()  # 关闭连接

