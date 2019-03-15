#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/9 9:25
import os
#取line的hash，对5000取余得到hashId，
# 写入到不同的文件，文件名{type}_{hashId}.txt
def write_temp(line,type,filePath):
    hashId = hash(line) % 5000
    temp_file =f"{filePath}//question2//{hashId}.txt"
    if not os.path.exists(temp_file):
        with open(temp_file,'w') as f:
            pass
    with open(temp_file,'a') as writer:
        writer.write(line)
    print(hashId,line)

#逐行读取文件
def readOneLine():
    file = "e://test//checkpoint//word.txt"
    hash_file = "e://test//checkpoint"
    with open(file,'r') as reader:
        lines = reader.readline()
        write_temp(lines, "word", hash_file)
        while lines:
            lines = reader.readlines(16*10000)
            for line in lines:
                  write_temp(line, "word", hash_file)

if __name__=="__main__":
    readOneLine()
