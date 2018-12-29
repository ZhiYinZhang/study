#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2018/12/21 17:05
import configparser as cp
import re
import time

def changeValue(key_value:dict,filePath):
    with open(filePath, 'r') as reader:
        lines = reader.read()
    #修改的次数
    x = 0
    for line in lines.splitlines():
        #修改完就退出循环
        if x == len(key_value.keys()):
            break
        for key in key_value.keys():
            if re.findall('%s'%key,line):
                lines = lines.replace(line,'%s    %s'%(key,str(key_value[key])))
                # print(key)
                x += 1
                #匹配到就退出
                break

    with open(filePath, 'w') as writer:
        writer.write(lines)


if __name__=="__main__":
    # num = [1,2,3,4]
    # cores = [1,2,3,4]
    # memory = ['10g','9g','8g','7g']
    #
    # filePath1 = "e://javacode/spark.conf"
    # num_key = 'hibench.yarn.executor.num'
    # cores_key = 'hibench.yarn.executor.cores'
    # memory_key = 'spark.executor.memory'
    #
    #
    # filePath2 = "e://javacode/hibench.conf"
    # dataSize_key = "hibench.scale.profile"
    # dataSize = [" tiny", "small", "large", "huge", "gigantic", "bigdata"]
    #
    # newValue={dataSize_key:dataSize[1]}
    # changeValue(newValue,filePath2)

    # for i in range(len(num)):
    #     time.sleep(3)
    #     d = {num_key:num[i],cores_key:cores[i],memory_key:memory[i]}
    #     changeValue(d,filePath1)

    # import sys
    #
    # for i in range(11):
    #     rate = (i) / 10
    #     num = int(rate * 100)
    #     # sys.stdout.write(f"\r[{'#'*num}{' '*(100-num)}]")
    #     # sys.stdout.flush()
    #     time.sleep(1)
    #     sys.stdout.write(f"\r [{'#'*num}{' '*(100-num)}] {rate*100}%")
    #     sys.stdout.flush()

     pass

