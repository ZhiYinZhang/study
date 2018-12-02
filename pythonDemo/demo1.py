#-*- coding: utf-8 -*-
# @Time    : 2018/6/27 22:52
# @Author  : Z
# @Email   : S
# @File    : demo1.py
import os,time
import threading
import elasticsearch
from pykafka import KafkaClient
def getDate(seconds):
    # current_time=time.time()
    date= time.strftime("%Y%m%d%H%M", time.localtime(seconds))
    return int(date)
def deleteExpire(expire_time):
    expire_file_path=f"{path}/log_{expire_time}.txt"
    if os.path.exists(expire_file_path):
        os.remove(expire_file_path)
path="e:/javacode/log"
curr_time=time.time()

start_date=getDate(curr_time)
end_date=getDate(curr_time+3*60)
expire_date=getDate(curr_time-6*60)

data=[]
i=0
while 1:
    # time.sleep(1)
    curr_time=time.time()
    curr_date=getDate(curr_time)
    if curr_date>=end_date:
        start_date=curr_date
        end_date=getDate(curr_time+3*60)
        expire_date=getDate(curr_time-6*60)
        deleteExpire(expire_date)
    data.append(str(i)+'\n')
    if len(data)==100:
        with open(f"{path}/log_{start_date}.txt",'a') as file:
            file.writelines(data)
            del(data[:])

    i+=1
    print(f"start:{start_date} end:{end_date} current:{curr_date} expire:{expire_date}")

