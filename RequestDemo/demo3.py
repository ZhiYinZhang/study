#-*- coding: utf-8 -*-
# @Time    : 2018/10/5 22:29
# @Author  : Z
# @Email   : S
# @File    : demo3.py

import os
import signal
import requests as rq
from bs4 import BeautifulSoup

response = rq.get(url ="https://item.jd.hk/33393314844.html#crumb-wrap")
html  = response.text
bs = BeautifulSoup(html,"html.parser")

result = bs.find_all(attrs={"class":"price J-p-33393314844"})
print(result[0].get_text())
