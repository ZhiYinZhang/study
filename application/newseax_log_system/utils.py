#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/11/19 11:15
from elasticsearch import Elasticsearch
import json
from datetime import datetime,timedelta
import re
import traceback as tb
import os
import threading
# 转成秒
date_map = {
    "d": 24 * 60 * 60,
    "h": 60 * 60,
    "m": 60,
    "s": 1
}
format_map = {
    "d":"%Y%m%d",
    "h":"%Y%m%d%H",
    "m":"%Y%m%d%H%M",
    "s":"%Y%m%d%H%M%S"
}

def createIndex(param: dict, esClient: Elasticsearch):
    """
          创建新index
    :param param:
    :param esClient:
    :return: index
    """
    topic = param['topic']
    now_time = datetime.now()
    unit = param['unit']
    str_time = now_time.strftime(format_map[unit])
    # index=topic_{str_time}
    new_index = '%s_%s' % (topic, str_time)

    threshold = param['interval'] * date_map[unit]

    if now_time.timestamp() - param['last_time'] >= threshold:  # 是否超过阈值
        if not esClient.indices.exists(index=new_index):
            # 创建新索引
            esClient.indices.create(index=new_index)

            param['last_index'] = new_index
            param['last_time'] = get_index_createTime(esClient, new_index,param)
            writeCheckpoint(param)
            print(f"创建ES新index：{new_index}")
            # 删除过期索引
            del_expire_index(param=param, esClient=esClient)
    else:
        new_index = param['last_index']

    return new_index

def get_app_indexs(topic,esClient:Elasticsearch):
    """
    :param topic
    :param esClient:
    :return: 倒序返回该topic在Elasticsearch所有的index
    """
    # ES所有的index
    all_index = esClient.cat.indices(h=['index']).split("\n")
    # 该topic的所有index
    app_indexs = []
    for index in all_index:
        try:
            if re.search(r"^(%s_)\d{6,14}" % topic, index):
                app_indexs.append(index)
        except Exception:
            tb.print_exc()
    # 倒序  最近的排前面
    app_indexs.sort(reverse=True)
    return app_indexs

def get_index_createTime(esClient,index,param):
    """
        返回index创建所属时间区域
        如：index创建时间为2018-11-19 14:25:40
           unit为'h':
               那么就为2018-11-19 14:00:00
           unit为'd':
               2018-11-19 00:00:00
           然后返回对应timestamp
    :param esClient:
    :param index:
    :return:
    """
    date_format = format_map[param['unit']]
    #获取该index的settings信息
    result = esClient.indices.get_settings(index=index)
    for i in result.values():
        #获取该index创建的时间戳
        create_time = int(i['settings']['index']['creation_date']) / 1000
    #将时间戳按照date_format格式化
    str_time = datetime.fromtimestamp(create_time).strftime(date_format)
    #再将格式化后的日期转成timestamp
    last_time = datetime.strptime(str_time,date_format).timestamp()
    return last_time

def get_index(topic,date_format):
    """
        根据date_format拼接index
    :param topic:
    :param date_format:
    :return:
    """
    now_time = datetime.now()
    str_time = now_time.strftime(date_format)
    # index=topic_{str_time}
    new_index = '%s_%s' % (topic, str_time)
    return new_index

def del_expire_index(param,esClient:Elasticsearch):
    """
        根据param的retain参数
        删除多余的index
    :param param:
    :param esClient:
    """
    app_indexs = get_app_indexs(param['topic'],esClient)

    print(f"all index:{app_indexs}")
    for expire_index in app_indexs[param['retain']:]:
        esClient.indices.close(index=expire_index)
        esClient.indices.delete(index=expire_index)
        print(f"删除ES过期index:{expire_index}")


# def writeLog(message,esClient):
#     if not esClient.indices.exists(index='dispatcher_log'):
#         body = {"mappings": {
#             "log": {
#                 "dynamic_date_formats": ["yyyy-MM-dd HH:mm:ssZ"]
#             }
#         }
#         }
#         #创建新索引
#         esClient.indices.create(index="dispatcher_log",body=body)
#     body={'datetime':'%s+0800'%time.strftime('%Y-%m-%d %H:%M:%S'),'message':message}
#     action={'_op_type':'index','_index':'log_level','_type':'doc','_source':body}
#     helpers.bulk(es,[action,])



"""-------------------------------------------------------------------------------------------------------------------------------------------------------"""

def readCheckpoint():
    """
     读取 checkpoint目录下所有topic信息
    :return:
    """
    path = "./checkpoint"
    params = []
    if os.path.exists(path):
          for p in os.listdir(path):
              with open(f"{path}/{p}",'r') as file:
                  params.append(json.load(file))
    return params

def writeCheckpoint(param):
    """
       保存topic的信息
    :param param:
    """
    path = f"./checkpoint/{param['topic']}.json"
    with open(path,mode='w') as file:
        json.dump(param,file)

def delete(topic,topics):
    """
       从正在消费的topic中移除
    :param topic: 要移除的
    :param topics: 所有正在消费的
    """
    if topic in topics.keys():
        #退出线程
        topics[topic].Flag()
        #从topics移除
        del(topics[topic])

        os.remove(f"./checkpoint/{topic}.json")
        print(f'{topic} delete success!')
    else:
        print('error: %s not exists'%topic)
#查询
def query(topics):
     #记录在topics字典中所有的topic
     all_topic=topics.keys()
     #当前进程所有的线程
     all_thread=threading.enumerate()
     #活着的topic消费线程
     active_topic=[]
     for i in all_topic:
         if topics[i] in all_thread:
             active_topic.append(i)
     print(active_topic)

if __name__=="__main__":
    pass