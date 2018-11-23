#-*- coding: utf-8 -*-
# @Time    : 2018/11/18 17:55
# @Author  : Z
# @Email   : S
# @File    : demo2.py
import hdfs
import json
import subprocess as sp
a= '{"a":1,"b":2}'
n = 1
b = f"""
n = {n}
a = '{a}'
"""

print(b)
