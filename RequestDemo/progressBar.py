#-*- coding: utf-8 -*-
# @Time    : 2018/12/2 9:16
# @Author  : Z
# @Email   : S
# @File    : progressBar.py
#下载文件时显示进度条
import requests
import sys
from datetime import datetime
def download_file(file_url,file_path,file_name):
    response = requests.get(url=file_url,stream=True)
    total_size = int(response.headers['Content-Length'])
    print(f"{file_name}文件大小：{total_size}")

    start_time = int(datetime.now().timestamp())
    print("开始下载。。。。。。")
    temp_size = 0
    with open(file_path,'wb') as f:
        for chunk in response.iter_content(chunk_size=10240):
            temp_size +=len(chunk)
            #done是已经下载的进度条长度，50是进度条的长度
            done = int(100*(temp_size/total_size))
            f.write(chunk)
            f.flush()

            now_time = int(datetime.now().timestamp())
            diff = now_time-start_time
            hour = diff //60//60
            min = diff //60
            seconds = diff-hour*60*60+min*60

            rate = round(100*temp_size/total_size,2)
            sys.stdout.write(f"\r已经下载{hour}小时{min}分钟{seconds}秒 [{'#'*done}{' '*(100-done)}] {rate}%")
            sys.stdout.flush()
    print()
if __name__=="__main__":
    file_url = "https://archive.apache.org/dist/hive/hive-3.1.1/apache-hive-3.1.1-bin.tar.gz"
    file_name = file_url.split("/")[-1]
    file_path = f"E:/test/{file_name}"

    download_file(file_url=file_url,file_path=file_path,file_name=file_name)