#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2018/12/11 17:31
import requests as rq
import time
import threading
from utils.split_integer import split_range

def continue_download(total_size):
    total = total_size
    x = 0
    y = 0
    interval = 1000
    for i in range(total // interval):
        x = i * interval
        y = x + (interval - 1)
        print(x, y)
        headers = {"Range": f"bytes={x}-{y}"}
        rp = rq.get(image_url, headers=headers)
        multi_thread_download(rp.content)
    #将剩余部分下载完
    x = y + 1
    y = total
    print(x, y)
    headers = {"Range": f"bytes={x}-{y}"}
    rp = rq.get(image_url, headers=headers)
    multi_thread_download(rp.content)

def get_size(file_url):
    try:
        rp = rq.head(url=file_url)
        total_size = int(rp.headers['Content-Length'])
    except:
        # 正常请求，没有Content-Length字段。 通过请求一部分文件内容
        headers = {"Range": f"bytes={0}-{1}"}
        h = rq.head(url=file_url, headers=headers)

        #  'Content-Range': 'bytes 0-1/26269919'
        total_size = int(h.headers['Content-Range'].split('/')[1])
    return total_size

def multi_thread_download(data):
    with open("e://test//text//text.jpg",'ab+') as writer:
        writer.write(data)

class myThread(threading.Thread):
    def __init__(self,data:dict,Range,thread_name,file_url):
        threading.Thread.__init__(self)
        self.data = data
        self.thread_name = thread_name
        self.Range = Range
        self.file_url = file_url
    def run(self):
        print(self.thread_name,self.Range)
        headers = {"Range":f"bytes={self.Range[0]}-{self.Range[1]}"}
        rp = rq.get(url=self.file_url,headers=headers)

        self.data[self.thread_name] = rp.content



if __name__=="__main__":
    file_url="https://archive.apache.org/dist/hive/hive-3.1.1/apache-hive-3.1.1-src.tar.gz"
    file_url1 = "http://fdfs-test.entrobus.com/group1/M00/03/6F/CmhqV1wON1eAOUPpAAV6jNM5JUY563.txt" #359052
    image_url = "http://pic24.nipic.com/20121010/3798632_184253198370_2.jpg"  # 283534


    url = file_url
    data = {}


    start1 = time.time()
    r1 = rq.get(url=url).content
    stop1 = time.time()

    start2 = time.time()
    #获取文件大小
    total_size = get_size(url)
    print(total_size)

    #将文件切成10份
    ranges = split_range(total_size,10)
    print(ranges)

    i = 0
    threads = {}
    for r in ranges:
        print('Thread%s'%str(i))
        thread_name = 'Thread%s'%str(i)
        th = myThread(data,r,thread_name,url)
        threads[thread_name] = th
        th.start()
        i += 1
    for thread in threads.keys():
        threads[thread].join()

    stop2 = time.time()

    print(stop1-start1,stop2-start2)
    keys = sorted(data.keys())
