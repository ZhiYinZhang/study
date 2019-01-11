#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/9 9:25
import os
#取line的hash，对5000取余得到hashId，
# 写入到不同的文件，文件名{type}_{hashId}.txt
def write_temp(line,type,filePath):
    hashId = hash(line) % 5000
    temp_file =f"{filePath}//{type}//{type}_{hashId}.txt"
    if not os.path.exists(temp_file):
        with open(temp_file,'w') as f:
            pass
    with open(temp_file,'a') as writer:
        writer.write(line)
    print(hashId,line)

#逐行读取文件
def readOneLine():
    file = "e://test//checkpoint1//word.txt"
    hash_file = "e://test//checkpoint1"
    with open(file,'r') as reader:
        line = reader.readline()
        while line:
            line = reader.readline()
            write_temp(line, "word", hash_file)

if __name__=="__main__":
    readOneLine()