#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2018/12/21 17:05
import configparser as cp
import re
import time

def changeValue(num,cores,memory,c):

    filePath = "e://test//spark.conf"

    with open(filePath,'r') as reader:
        lines = reader.read()

    # hibench.yarn.executor.num = 8
    # hibench.yarn.executor.cores = 10
    # spark.executor.memory  10g
    num_key = 'hibench.yarn.executor.num'
    cores_key = 'hibench.yarn.executor.cores'
    memory_key = 'spark.executor.memory'

    # print(lines)
    i = 0
    for line in lines.splitlines():
        if i == 3:
            break
        if re.findall('%s'%num_key,line):
            lines = lines.replace(line,'%s      %s'%(num_key,str(num)))
            i+=1
            continue

        if re.findall('%s'%cores_key,line):
            lines = lines.replace(line,'%s      %s'%(cores_key,str(cores)))
            i+=1
            continue

        if re.findall('%s'%memory_key,line):
            lines = lines.replace(line,'%s      %s'%(memory_key,memory))
            i+=1
            continue

    # print(lines)

    with open(filePath,'w') as writer:
        writer.write(lines)


if __name__=="__main__":
    num = [1,2,3,4]
    cores = [1,2,3,4]
    memory = ['10g','9g','8g','7g']

    for i in range(len(num)):
        time.sleep(5)
        changeValue(num[i],cores[i],memory[i],1)
        print(num[i],cores[i],memory[i])
