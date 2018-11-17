#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/10/29 11:26
import time
import datetime
import json
import os
import requests
result=b'Total number of applications (application-types: [] and states: [SUBMITTED, ACCEPTED, RUNNING]):1\n                Application-Id\t    Application-Name\t    Application-Type\t      User\t     Queue\t             State\t       Final-State\t       Progress\t                       Tracking-URL\napplication_1539249240481_0434\t          CoreApp.py\t               SPARK\t   zhangzy\troot.userpool.zhangzy\t           RUNNING\t         UNDEFINED\t            10%\t            http://Entrobus11:37047\n'

result = result.__str__()[2:-1]


result = result.split('\\n')

for i in result:
    print(i)


apps=[]

for i in result[2:-1]:
    param = []
    for j in i.split('\\t'):
        param.append(j.strip(' '))
    if param[3]=="zhangzy":
        d = {"Application-Id":param[0],
         "Application-Name":param[1],
         "Application-Type":param[2],
         "User":param[3],
         "Queue":param[4],
         "State":param[5],
         "Final-State":param[6],
         "Progress":param[7],
         "Tracking-URL":param[8]}
        apps.append(d)
print(apps)