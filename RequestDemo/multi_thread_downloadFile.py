#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2018/12/11 17:31
import requests as rq
import time
import threading


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

def multi_thread_download(data):
    with open("e://test//text//text.jpg",'ab+') as writer:
        writer.write(data)

class myThread(threading.Thread):
    def __init__(self,data:dict,Range):
        threading.Thread.__init__(self)
        self.data = data
    def run(self):
        pass



if __name__=="__main__":
    file_url="https://archive.apache.org/dist/hive/hive-3.1.1/apache-hive-3.1.1-src.tar.gz"
    file_url1 = "http://fdfs-test.entrobus.com/group1/M00/03/6F/CmhqV1wON1eAOUPpAAV6jNM5JUY563.txt" #359052
    image_url = "http://pic24.nipic.com/20121010/3798632_184253198370_2.jpg"  # 283534

    headers = {"Range": f"bytes={0}-{10000}"}
    start = time.time()
    # h = rq.head(url=file_url)
    rp = rq.get(url=file_url1)
    result = rp.content
    stop = time.time()
    print("time:",stop-start)
    print(rp.headers)




    href = "http://10.18.0.28:50070/webhdfs/v1/user/zhangzy/error.txt?op=OPEN" #759
    # x = 0
    # y = 500000
    headers = {"Range":f"bytes={0}-{1}"}
    rp = rq.get(href,headers=headers)
    with open("e://test//public//error1.txt",'ab+') as writer:
        # writer.seek(200000)
        writer.write(rp.content)


    # interval = 100
    # x = 0
    # d = {}
    # # l = [('a',x+0*interval),('b',x+1*interval),(x+2*interval)]
    # l = [('a', 0), ('b', x + 100000), ('c',x + 200000)]
    # for i,j in l:
    #     headers = {"Range":f"bytes={j}-{j+99999}"}
    #     rp = rq.get(image_url,headers=headers)
    #     d[i] = rp.content
    #
    # total = b''
    # for key in d.keys():
    #     total += d[key]
    #
    # print(len(total))
    # with open("e://test//text//text.jpg",'ab+') as writer:
    #     writer.write(total)