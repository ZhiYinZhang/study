#-*- coding: utf-8 -*-
# @Time    : 2018/10/21 21:01
# @Author  : Z
# @Email   : S
# @File    : demo2.py
import json
import time
with open("e:/javacode/test.json",'r') as file:
    d = json.load(file)
process = d['process']

process_sort = []
for action1 in process:

    flag = True
    for action2 in process:
        if action1['outputDatasets'][0] in action2['inputDatasets']:
                 flag = False
    #flag为True:该操作的输出没有作为过输出，是最后的操作
    if flag:
        process_sort.append(action1)

print(process_sort)