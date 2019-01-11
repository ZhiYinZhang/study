#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/4 17:18
import requests as rq
from bs4 import BeautifulSoup
from bs4.element import Tag
import re
from RequestDemo.progressBar import *
"""
在pypi网站下载python模块的离线jar包

"""
app = ['absl-py', 'appdirs', 'argcomplete', 'astor', 'attrs', 'bitcoin', 'black', 'boto3', 'botocore', 'bz2file',
       'configparser', 'databricks-cli', 'dist-keras', 'docopt', 'elephas', 'fastparquet', 'fbprophet', 'future',
       'gast', 'gensim', 'gitdb2', 'GitPython', 'grpcio', 'gunicorn', 'hdfs', 'hyperas', 'hyperopt', 'jmespath',
       'jnius', 'JPype1', 'jupyter', 'Keras', 'Keras-Applications', 'Keras-Preprocessing', 'lightgbm', 'Markdown',
       'mleap', 'mlflow', 'nose-exclude', 'parso', 'pika', 'pluggy', 'protobuf', 'py4j', 'pyarrow', 'pydl4j',
       'pyhanlp', 'pymongo', 'pyspark', 'pystan', 'pytest-runner', 'querystring-parser', 's3transfer', 'sacrebleu',
       'Send2Trash', 'simplejson', 'smart-open', 'smmap2', 'tabulate', 'tensorboard', 'tensorflow', 'tensorflowonspark',
       'termcolor', 'Theano', 'thrift', 'toml', 'torch', 'torchvision', 'sklearn','statsmodel','xgboost','lightgbm',
       'pytorch','torchvision','tensorflow','keras', 'hmmlearn', 'prophet']

file_path = "E:\\python库\\"

for i in app[19:]:
    try:
        url = "https://pypi.org/project/%s/#files"%i
        response = rq.get(url=url)
        html = response.text
        bs = BeautifulSoup(html,"html.parser")
        result = bs.find_all(name="a",attrs={'href':re.compile(r"(tar.gz)$")})

        app_url = result[0].get("href")
        print(app_url)
        rq.get(app_url)

        name = app_url.split("/")[-1]


        download_file(file_url=app_url,file_path=file_path+name,file_name=name)
    except:
        print("失败:",i)


