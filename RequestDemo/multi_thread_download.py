#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2018/12/11 17:31
import requests as rq
import time
import threading
import math

def split_ranges(total,partition):
    ranges=[]
    if total>2*partition:
        partition_size=math.ceil(total/partition)
        start=0
        end=0
        for i in range(10):
            start=i*partition_size
            end=start+(partition_size-1)
            #最后一个
            if (i+1)==partition:
                end=total
            ranges.append((start,end))
    else:
        ranges.append((0,total))
    return ranges

def get_size(file_url):
    """
    获取下载内容大小
    :param file_url:
    :return:
    """
    try:
        #获取reponse的header
        rp = rq.head(url=file_url)
        #从header里面提取内容大小
        total_size = int(rp.headers['Content-Length'])
    except:
        # 正常请求，没有Content-Length字段。 可以通过请求一部分文件内容
        headers = {"Range": f"bytes=0-1"}
        h = rq.head(url=file_url, headers=headers)

        #  'Content-Range': 'bytes 0-1/26269919'
        total_size = int(h.headers['Content-Range'].split('/')[1])
    return total_size

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


def mutil_doanload(url,file_path,threads):
    # 获取文件大小
    total_size = get_size(url)
    print(f"total_size:{total_size}")

    # 将文件切成10份
    ranges = split_ranges(total_size, threads)
    print(ranges)

    data = {}
    #thread number
    i = 0
    threads = {}
    for r in ranges:
        # print('Thread%s' % str(i))
        thread_name = 'Thread%s' % str(i)
        th = myThread(data, r, thread_name, url)
        threads[thread_name] = th
        th.start()
        i += 1
    for thread in threads.keys():
        threads[thread].join()


    keys = sorted(data.keys())

    for key in keys:
        with open(file_path, 'ab+') as writer:
            writer.write(data[key])



if __name__=="__main__":
    file_url="https://archive.apache.org/dist/hive/hive-3.1.1/apache-hive-3.1.1-src.tar.gz"
    file_url1 = "http://fdfs-test.entrobus.com/group1/M00/03/6F/CmhqV1wON1eAOUPpAAV6jNM5JUY563.txt" #359052
    image_url = "http://pic24.nipic.com/20121010/3798632_184253198370_2.jpg"  # 283534


    url = image_url

    file_path="e://test//temp//text1.jpg"
    mutil_doanload(url,file_path,threads=10)
