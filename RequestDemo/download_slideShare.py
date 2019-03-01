#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/9 10:40
import os
import re
import requests as rq
import sys
import time

#0-250000000 250000001-500000000 500000001-750000000 750000001-975188061

if __name__=="__main__":
    url = "https://downloads.dcos.io/dcos/stable/dcos_generate_config.sh"
    rp = rq.head(url)
    # size 975188061
    print(rp.headers["Content-Length"])

    headers = {"Range": "bytes=0-500000"}
    rp = rq.get(url=url, headers=headers)
    # #
    path = "E:\\test\\url"

    temp_size = 0
    with open(path+"//1.sh",'wb+') as writer:
             writer.write(rp.content)




    # total = b''
    # for i in os.listdir(path):
    #     with open(path+f"//{i}","rb") as reader:
    #         content = reader.read()
    #     total+=content
    #
    # with open(path+"//4.jpg","wb") as writer:
    #     writer.write(total)