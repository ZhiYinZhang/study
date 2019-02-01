#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/30 18:51
import time

filepath = "E:\\test\\csv\\fang_apt_changde.txt"

for i in range(10):
    time.sleep(5)
    with open(filepath,"r",encoding='utf-8') as reader:
        result = reader.readlines()


    with open(f"E:\\test\csv\stream\\{i}.txt","w",encoding='utf-8') as writer:
        writer.writelines(result)


print(result)