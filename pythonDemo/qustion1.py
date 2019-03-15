#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/8 17:44
import os
import threading
import psutil
# question1:
# 给定a、b两个文件，各存放50亿个url，每个url各占64字节，内存限制是4G，找出a、b文件共同的url？
"""
文件大小50*(10**8)*64/(1024**3)=298G,远远大于内存限制的4g，只能分隔成小文件来处理
1.遍历a文件，逐行读取，对每个url求hash(url)%1000,根据hashId将url分别存储到1000个小文件（a0,a1..a999,每个文件约300M）
2.遍历b文件，采取和a相同的方式,存到1000个小文件中，（b0,b1...b999）
经过这样处理后，所有可能相同的url都被保存到对应的1000个小文件中（a0-b0,a1-b1,..a999-b999）,
不对应的文件中不可能有相同的url，然后只要求对应的文件中相同的url即可
"""
#取line的hash，对5000取余得到hashId，
# 写入到不同的文件，文件名{type}_{hashId}.txt


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
        #读取文件
        with open(filePath + f"\\{tp}.txt", 'r') as reader1:
            #每个url  64bytes
            #第一次读取 判断是否有数据
            lines = reader1.readline()
            write_temp(lines, f"{tp}", filePath)
            #有数据 进入循环
            while lines:
                lines = reader1.readlines(64*1000)
                for line in lines:
                     write_temp(line, f"{tp}", filePath)

def print_mem():
    """
      获取当前进程的内存信息
    :return:
    """
    process = psutil.Process(os.getpid())
    print(process.memory_info())


filePath = "E:\\test\\checkpoint"


if __name__=="__main__":
    print_mem()
    types = ["url1",'url2']
    #------每个url文件开启一个线程 写小文件-----
    # for i in types:
    #     t = myThread(tp=i)
    #     t.start()


    #------比较对应小文件的url------

    #小文件的目录
    url1_path=os.path.join(filePath,types[0])
    url2_path = os.path.join(filePath, types[1])
    #所有小文件
    url1_files=os.listdir(url1_path)
    url2_files=os.listdir(url2_path)
    print_mem()
    #遍历获取对应的小文件
    for i in range(len(url1_files)):
        #一次将两个小文件的内容都读进来
        # 或 读取一个文件的全部url，然后在循环里面读取另一个文件，每次读一行，如果在集合里面就保存

        #读取url1的第i个小文件 约10M
        with open(os.path.join(url1_path,url1_files[i]),"r") as reader1:
            url1s=set(reader1.readlines())

        print_mem()

        # 读取url2的第i个小文件 约10M
        with open(os.path.join(url2_path,url2_files[i]),"r") as reader2:
            url2s=set(reader2.readlines())

        print_mem()

        #求两个set的交集
        result=url1s.intersection(url2s)

        print_mem()

        with open(os.path.join(filePath,"result.txt"),"a") as writer:
                writer.write(f"--------------------------第{i}个文件有{len(result)}相同的url------------------------------\n")
                writer.writelines(result)

        print_mem()

        print(f"第{i}个文件有{len(result)}相同的url")


