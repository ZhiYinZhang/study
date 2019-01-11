#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/11 14:59

file ="E:\资料\Databricks"


with open(file+"\en.txt",'r',encoding='utf-8') as reader1:
    line1 = reader1.readlines()

with open(file+"\zh.txt",'r',encoding='utf-8') as reader2:
    line2 = reader2.readlines()


line3 = []
for i in range(len(line1)):
    line3.append(line1[i])
    line3.append(line2[i])

with open(file+"\en_zh.txt",'w') as write:
    write.writelines(line3)
