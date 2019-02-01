#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/29 14:39
from hdfs import client
from datetime import datetime
import os
import subprocess as sp
import time
import traceback as tb

def get_hdfs_client(hdfs_host,user):
    """

    :return:
    """
    cli = client.InsecureClient(url=hdfs_host, user=user)
    return cli


def upload_hdfs(localPath, hdfsPath,**kwargs):
    """

    :param localPath: 本地绝对路径
    :param hdfsPath: hdfs绝对路径
    :param kwargs: 'hdfs_host':hdfs的http  http://localhost:50070
                   'user'
    """
    hdfs_host = kwargs.get("hdfs_host","http://entrobus28:50070")
    user = kwargs.get("user","zhangzy")

    cli = get_hdfs_client(hdfs_host, user)

    print("%s 将{%s}上传到dhfs的{%s}"%(datetime.now().strftime("%F %X.%f"),localPath, hdfsPath))

    cli.upload(hdfs_path=hdfsPath, local_path=localPath,overwrite=True,permission=777)
    print("%s 上传完成"%datetime.now().strftime("%F %X.%f"))



if __name__ == "__main__":
    path = "/home/zhangzy/dataset/smartCity/"
    local_temp_path = path + "temp/"
    hdfsPath = f"/user/zhangzy/smartCity/sourceData/"
    old_line = 0
    while True:
        try:
            # filePath = f"/home/zhangzy/dataset/smartCity/20190131/fang_zu{20190130}.csv"
            d = datetime.now().strftime("%Y%m%d")
            local_file_path = f"{path}{d}/fang_zu{d}.csv"



            if os.path.exists(local_file_path):
                r = sp.check_output("wc -l %s|awk '{print $1}'" % local_file_path, shell=True)
                current_line = int(r[:-1])

                diff = current_line - old_line
                # 可能换了一个文件
                if diff < 0:
                    print("小于")
                    old_line = 0
                    diff = current_line
                if diff > 0:
                    print("大于")
                    with open(local_file_path, "r") as reader:
                        lines = reader.readlines()[old_line:-1]

                    t = datetime.now().strftime("%Y%m%d%H%M%S")

                    if not os.path.exists(local_temp_path):
                        os.makedirs(local_temp_path)
                    local_temp_file = local_temp_path+f"{t}.csv"
                    hdfs_file = hdfsPath+f"{t}.csv"

                    with open(local_temp_file, "w") as writer:
                        writer.writelines(lines)

                    upload_hdfs(local_temp_file, hdfs_file)

                    old_line = current_line
            print(f"fang_zu{d}.csv old_line:{old_line},current_line:{current_line}")
        except:
            tb.print_exc()
        time.sleep(5)