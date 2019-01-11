#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/8 17:44
import os
import threading
def write_temp(url,type,filePath):
    hashId = hash(url) % 100
    temp_file =f"{filePath}//{type}//{type}_{hashId}.txt"
    if not os.path.exists(temp_file):
        with open(temp_file,'w') as f:
            pass
    with open(temp_file,'a') as writer:
        writer.write(url)
    print(type,hashId,url)

class myThread(threading.Thread):
    def __init__(self,tp):
         threading.Thread.__init__(self)
         self.tp=tp
    def run(self):
        tp = self.tp
        with open(filePath + f"\\{tp}.txt", 'r') as reader1:
            line = reader1.readline()
            write_temp(line, f"{tp}", filePath)
            while line:
                line = reader1.readline()
                write_temp(line, f"{tp}", filePath)

filePath = "E:\\test\\checkpoint1"


if __name__=="__main__":
    types = ["url1",'url2']
    for i in types:
        t = myThread(tp=i)
        t.start()

