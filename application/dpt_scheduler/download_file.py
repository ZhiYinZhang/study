#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/11/27 14:53
import time
import requests
import os
import hdfs
import json
import sys
def get_hefsClient():
    url = "http://10.18.0.11:50070"
    user = 'zhangzy'
    root = '/user/zhangzy'
    return hdfs.InsecureClient(url =url,user=user,root=root)

def download_2_hdfs(file_url,file_name):
    print(f"文件地址：{file_url}")
    print(f"文件名：{file_name}")
    print("开始下载。。。。。。")
    response = requests.get(url=file_url,stream=True)

    origin_file_size = int(response.headers['Content-Length'])
    print(f"原始文件大小：{origin_file_size}")

    print("获取hdfs的client。。。。。。")
    hdfs_client = get_hefsClient()
    hdfs_filePath = f"./{file_name}"


    temp_size = 0
    for chunk in response.iter_content(chunk_size=4096):
        if chunk:
            temp_size += len(chunk)

            hdfs_client.write(hdfs_path=hdfs_filePath, data=chunk,append=True)

            done = 50*temp_size//origin_file_size
            rate = round(100*temp_size/origin_file_size,2)
            sys.stdout.write(f"\r[{'#'*done}{' '*(50-done)}] {rate}%")
            sys.stdout.flush()
    print()

    print("上传完成")
    hdfs_file_size = hdfs_client.status(hdfs_filePath)['length']
    print(f"上传到hdfs后文件大小：{hdfs_file_size}")
    if origin_file_size == hdfs_file_size:
        global flag
        flag = False
    else:
        print("下载失败")

if __name__=="__main__":
        file_url = "https://archive.apache.org/dist/hive/hive-3.1.1/apache-hive-3.1.1-src.tar.gz"
        file_name = file_url.split("/")[-1]
        # try:
        #    get_hefsClient().delete('./apache-hive-3.1.1-src.tar.gz')
        # except:
        #     pass
        flag = True
        for i in range(3):
           if flag==True:
             download_2_hdfs(file_url,file_name)
           else:
               break

