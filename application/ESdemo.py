#!/usr/bin/env python
# -*- coding:utf-8 -*-
import pykafka
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import time
from datetime import datetime,timedelta
import json
import threading
import re
import sys
import os
import traceback as tb
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



class myThread(threading.Thread):
    def __init__(self,esClient,kafkaClient:pykafka.KafkaClient,param:dict,topics:list,metadata:list):
        threading.Thread.__init__(self)
        self.setName(param['topic'])

        self.esClient = esClient
        self.kafkaClient = kafkaClient
        self.param = param
        self.topics = topics
        self.metadata = metadata
        self.flag = False
    def run(self):
        print(f"start {self.param['topic']} thread")
        try:
            kafka2ES(self.esClient, self.kafkaClient, self.param,self.flag)
        except Exception:
            delete(self.param['topic'],self.topics,self.metadata)
            tb.print_exc()
        print(f"exit {self.param['topic']} thread")
    #线程是否退出的标志  True:退出
    def Flag(self):
        self.flag=True

#从kafka中消费消息并写入elasticsearch
def kafka2ES(esClient, kafkaClient, param:dict,flag:bool):
    """

    :param esClient: elasticSearch client
    :param kafkaClient:  kafka client
    :param topic:  topic
    :param batch: 每个批次的大小，即每次写多少条
    :param mythread:
    """
    #最近一次时间
    last_time = datetime.now().timestamp()
    print('start write %s'%topic)
    # 获取消费者
    consumer = kafkaClient.topics[bytes(topic,'utf-8')].get_simple_consumer()

    # 获取消费的消息
    actions = []
    print(f"开始消费{topic}")
    for message in consumer:
            if flag:
                sys.exit()
            #创建ES的index 和 删除过期的index
            index = createIndex(param=param, esClient=esClient,last_time=last_time)

            log = message.value.decode()
            body={"log":log}
            action={'_op_type':'index','_index':index,'_type':'log','_source':body}
            actions.append(action)
            # print(f"当前actions有{len(actions)}条数据，batch为{batch}")
            if len(actions) == param['batch']:
                print(f"开始写入ES---{index}---{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                helpers.bulk(esClient,actions)
                del(actions[:])
                print(f"写入ES完成---{index}---{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

def get_app_indexs(topic,esClient:Elasticsearch):
    """
    :param topic
    :param esClient:
    :return: 返回该topic在Elasticsearch所有的index
    """
    # ES所有的index
    all_index = esClient.cat.indices(h=['index']).split("\n")
    # 该topic的所有index
    app_indexs = []
    for index in all_index:
        try:
            if re.search(r"^(%s-log_)\d{6,14}" % topic, index):
                app_indexs.append(index)
        except Exception:
            tb.print_exc()
    # 倒序  最近的排前面
    app_indexs.sort(reverse=True)
    return app_indexs

#转成秒
date_map = {
    "d":24*60*60,
    "h":60*60,
    "m":60,
    "s":1
}
def createIndex(param:dict,esClient:Elasticsearch,last_time):
    """

    :param param:
    :param esClient:
    :param last_time: 上一次创建index的时间
    :return:
    """
    #设置index中日期的格式
    body = {"mappings": {
        "log": {
            "dynamic_date_formats": ["yyyy-MM-dd HH:mm:ssZ"]
        }
    }
    }
    topic = param['topic']
    now_time = datetime.now()

    str_time = now_time.strftime("%Y%m%d%H%M%S")
    # index=topic_{str_time}
    new_index = '%s_%s' % (topic, str_time)

    threshold = param['interval']*date_map[param['unit']]
    if now_time.timestamp() - last_time >= threshold:#是否超过阈值


        # 设置index的mapping，定义ES解析的日期格式   不存在就创建
        if not esClient.indices.exists(index=new_index):
            #创建新索引
            esClient.indices.create(index=new_index,body=body)
            print(f"创建ES新index：{new_index}")
            #删除过期索引
            del_expire_index(topic=topic,esClient=esClient)
    else:
        start = time.time()
        indexs = get_app_indexs(topic,esClient)
        if len(indexs) == 0:
            pass
        else:
            new_index = indexs[0]
        end = time.time()
    return new_index

def del_expire_index(topic,esClient:Elasticsearch):
    app_indexs = get_app_indexs(topic,esClient)

    print(f"all index:{app_indexs}")
    for expire_index in app_indexs[2:]:
        esClient.indices.close(index=expire_index)
        esClient.indices.delete(index=expire_index)
        print(f"删除ES过期index:{expire_index}")

#----------------------------------------------调度----------------------------------------------------------------

#读取checkpoint的topic
def readCheckpoint():
    path='./checkpoint/topic.txt'
    if os.path.exists(path):
        with open(path, mode='r') as file:
           return json.load(file)

    else:
        print('no such file: %s'%path)
#checkpoint
def writeCheckpoint(metadata):
    with open('./checkpoint/topic.txt',mode='w') as file:
        json.dump(metadata,file)

def delete(topic,topics,metadata):
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
        #从metadata中移除
        for i in metadata:
            if i['topic'] == topic:
                metadata.remove(i)
        #写入文件
        writeCheckpoint(metadata)
        print(f'{topic} delete success!')
    else:
        print('error: %s not exists'%topic)
#查询
def query():
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
#开启一个线程去消费topic
def open_thread(param, topics):
    topic = param['topic']
    if re.search(r'.+(-log)$', topic):#是否符合规则
        if bytes(topic,'utf-8') in kafka.topics.keys():#kafka中是否存在
            if topic not in topics.keys():#是否已经在消费
                thread_obj = myThread(esClient=es, kafkaClient=kafka, param=param,topics=topics,metadata=metadata)
                topics[topic] = thread_obj
                metadata.append(param)
                writeCheckpoint(metadata)
                thread_obj.start()
            else:
                print('warn : kafka topic %s is being comsumed' % topic)
        else:
            print('error: kafka topic %s not exist' % topic)
    else:
        print('error: kafka topic must end with "-log"')

def parse_param(param:dict):
    print(param)

if __name__=="__main__":
    metadata = []
    # 连接kafka
    kafka=pykafka.KafkaClient("10.18.0.11:9092,10.18.0.8:9092")
    # 连接ES
    es = Elasticsearch(hosts='http://119.29.165.154', port=9200)
    #存储所有消费的topic及对应的线程对象的字典
    topics = {}

    #从checkpoint文件读取上次保存的topic 并开启线程消费
    for i in readCheckpoint():
        if not re.search(r'\s+',i["topic"]):
             print("开启上一次")
             open_thread(param=i,topics=topics)

    #从管理topic(app)中获取应用topic名称   每个应用开启一个线程取消费
    consumer=kafka.topics[b'app'].get_simple_consumer()
    for messages in consumer:
        message=messages.value.decode()
        try:
          message:dict = json.loads(message)
          parse_param(message)

          command = message['command']
          if command== 'delete':
              topic = message['topic']
              delete(topic=topic,topics=topics,metadata=metadata)
          elif command == 'list':
              query()
          elif command == 'add':
              open_thread(param=message,topics=topics)
        except Exception:
            # tb.print_exc()
            print(message)
