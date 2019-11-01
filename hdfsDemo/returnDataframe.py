#-*- coding: utf-8 -*-
# @Time    : 2018/10/12 22:43
# @Author  : Z
# @Email   : S
# @File    : returnDataframe.py
import json
from hdfsDemo.hdfsClient import *
import pandas as pd
from hdfs.client import InsecureClient
from hdfs.ext.dataframe import read_dataframe,write_dataframe
def get_pd_DF(cli:InsecureClient,file_path):
    """
       读取hdfs上的csv文件，返回pandas的DataFrame
    :param cli: hdfs的InsecureClient
    :param file_path: hdfs的文件路径,相对InsecureClient里面设置的root路径
    :return:
    """
    with cli.read(file_path) as reader:
                df_pd = pd.read_csv(reader)
    return df_pd
def save_pd_DF(df_pd:pd.DataFrame,cli:InsecureClient,file_path):
    """
     将pandas的DataFrame写入hdfs  csv
    :param df_pd: pandas的DataFrame
    :param cli: hdfs的InsecureClient
    :param file_path: hdfs的文件路径,相对InsecureClient里面设置的root路径
    """
    with cli.write(hdfs_path=file_path, encoding='utf-8', overwrite=True) as writer:
        df_pd.to_csv(writer)
class Progress(object):
    def __init__(self):
        # self._data ={}
        pass
    def __call__(self, hdfs_path,nbytes):
        print(hdfs_path)
        print(nbytes)

import numpy as np
if __name__=="__main__""":
    cli = get_hdfs_client()
    file_path = "badisjob/DPT/user/21C5BCE840B3AE9170C5A69EF62B40E9/AD0152FFC33B47DE9D3B52F58849AF3D/18C3ADF70167AD9FA7CCBA60EEC3C75E.json"
    file_path = "badisjob/DPT/publicDataset/model01_train.csv"
    # get_pd_DF(cli,file_path)
    with cli.read(file_path,chunk_size=100,progress=Progress) as reader:
        pass
        # df = pd.read_csv(reader)

    # print(df.dtypes)


