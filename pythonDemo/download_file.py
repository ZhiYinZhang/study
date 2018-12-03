#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2018/11/30 14:55

#下载文件，实现断点续传，下载进度显示
import sys
import requests
import os

def download(url,file_path):
    #
    r1 = requests.get(url=url,stream=True)
    total_size = int(r1.headers['Content-Length'])

    if os.path.exists(file_path):
        temp_size = os.path.getsize(file_path)
    else:
        temp_size = 0

    print(temp_size)
    print(f"文件大小：{total_size}")

    # r1 = requests.get(url)
    with open(file_path,"ab") as f:
        for chunk in r1.iter_content(chunk_size=1024):
            if chunk:
                temp_size +=len(chunk)
                f.write(chunk)
                f.flush()

                done = int(50*temp_size/total_size)
                sys.stdout.write("\r[%s%s] %d%%" % ('█' * done, ' ' * (50 - done), 100 * temp_size / total_size))
                sys.stdout.flush()
    print()
if __name__=="__main__":
    file_url = "https://archive.apache.org/dist/hive/hive-3.1.1/apache-hive-3.1.1-src.tar.gz"

    file_name = file_url.split("/")[-1]
    file_path = f"e://test//text/{file_name}"


    download(url=file_url,file_path=file_path)