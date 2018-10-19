#!/usr/bin/env python
# -*- coding:utf-8 -*-
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
my_sender='zhangzy@entrobus.com'
my_pass='Zhangzhiying?35'
my_user='zhangzy@entrobus.com'

def mail():
    ret=True
    try:
        msg=MIMEText(_text='发送邮件内容',_charset='utf-8')
        msg['from']=formataddr(["张志颖",my_sender])
        msg['to']=formataddr(["张志颖",my_user])
        msg['Subject']='发送测试邮件'

        # 发件人邮箱的smtp服务器  端口
        server=smtplib.SMTP_SSL(host='smtp.exmail.qq.com',port=465)
        # 发件人邮箱和密码
        server.login(my_sender,my_pass)
        server.sendmail(my_sender,[my_user,],msg.as_string())

        server.quit()
    except Exception:
        ret=False
    return ret
ret=mail()
if ret:
    print('邮件发送成功')
else:
    print('邮件发送失败')