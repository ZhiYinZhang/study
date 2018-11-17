#!/usr/bin/env python
# -*- coding:utf-8 -*-
import pykafka
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import time
import threading
import re
import sys
import os

class myThread(threading.Thread):

    def __init__(self,esClient,kafkaClient,topic,number=500):
        threading.Thread.__init__(self)
        self.setName(topic)
        self.esClient=esClient
        self.kafkaClient=kafkaClient
        self.topic=topic
        #当日志有rate条时写入ES
        self.number=number
        self.flag=False

    def run(self):
        print("start.................%s"%self.topic)
        try:
            kafka2ES(self.esClient, self.kafkaClient, self.topic,self)
        except BaseException:
            # print(message)
            delete(self.topic,topics)
        print("exit..................%s"%self.topic)

    #线程是否退出的标志  True:退出
    def Flag(self):
        self.flag=True


def kafka2ES(esClient, kafkaClient, topic,mythread):
    """
    consume kafka topic message，and write to Elasticsearch
    :param esClient:
    :param kafkaClient:
    :param topic:
    :param mythread: thread instance
    """

    print('start write %s'%topic)
    # 获取消费者
    consumer = kafkaClient.topics[topic].get_simple_consumer()
    # 切割topic ‘appName-log’ 获取appName
    app=re.split(r'-log', topic)[0]
    # 获取消费的消息   不是标准的json格式，当做string
    parse_right_actions=[]
    parse_error_actions=[]
    for message in consumer:
        #判断线程退出标志
        if mythread.flag:
            print('terminate {topic}')
            sys.exit()

        #创建ES的index
        index = createIndex(app, esClient=esClient)
        log = message.value
        try:
            #将json字符串解析成字典
            body = eval(log)
            action={'_op_type':'index','_index':index,'_type':'log','_source':body}
            parse_right_actions.append(action)
            #每500条写一次
            if len(parse_right_actions) == 500:

                helpers.bulk(esClient,parse_right_actions)
                del(parse_right_actions[:])

        except:#解析错误
        #      try:
                 # print('解析错误--%s'%log)
                 parse_error={'level': 'parse failed', 'datetime':'%s+0800'%time.strftime('%Y-%m-%d %H:%M:%S'),'message': log}
                 parse_error_action={'_op_type':'index','_index':index,'_type':'log','_source':parse_error}
                 parse_error_actions.append(parse_error_action)
                 if len(parse_error_actions)==500:
                     print('开始写2')
                     helpers.bulk(esClient,parse_error_actions)
                     del(parse_error_actions[:])
                     print('写入完成2')
        #      except:
        #         print('写入错误')
        #         esClient.index(index=index, doc_type='log', body={"level":"write failed","message":log,"datetime":"%s+0800"%time.strftime('%Y-%m-%d %H:%M:%S')})


def createIndex(app,esClient):
    """
     create a new index and delete expired index
    :param app:  application's name
    :param esClient: Elasticsearch's client
    :return: return new Elasticsearch's index
    """

    # 设置index中的字段schema
    body = {"mappings": {
        "log": {
            "dynamic_date_formats": ["yyyy-MM-dd HH:mm:ssZ"]
        }
    }
    }
    # index=app_date      %Y%m%d%H%M 定义多久生成一个新的index
    now = time.strftime('%Y%m%d%H', time.localtime())
    index = '%s_%s' % (app, now)
    # 设置index的mapping，定义ES解析的日期格式   不存在就创建
    if not esClient.indices.exists(index=index):
        #创建新索引
        esClient.indices.create(index=index,body=body)
        # 过期的索引
        expire_index = '%s_%s' % (app, str(int(now) - 2))
        #判断过期的索引是否存在  存在就删除
        if esClient.indices.exists(index=expire_index):
            esClient.indices.close(index=expire_index)
            esClient.indices.delete(index=expire_index)
    return index



#----------------------------------------------调度----------------------------------------------------------------

def readCheckpoint():
    """
    读取checkpoint文件中的topic
    :return: 返回从checkpoint文件中读取的topic
    """
    path='./checkpoint/topic.txt'
    if os.path.exists(path):
        with open(path, mode='r') as file:
            topics = file.read().splitlines()
        return topics
    else:
        print('no such file: %s'%path)


def writeCheckpoint(topics):
    """

    :param topics:
    """
    with open('./checkpoint/topic.txt',mode='w') as file:
        for i in topics:
            file.write(i+'\n')


def delete(topic,topics):
    """
     delete topic consumer
    :param topic:  The topic you want to delete
    :param topics:
    """

    if topic in topics.keys():
        #退出线程
        topics[topic].Flag()
        #从topics中移除
        del(topics[topic])
        #保存到checkpoint文件
        writeCheckpoint(topics.keys())
        print(f'delete {topic} success!')
    else:
        print(f'error: {topic} not exists')


def query(topics):

     """
        query all alive topic
     """
     # 记录在topics字典中所有的topic
     all_topic=topics.keys()
     #当前进程所有的线程
     all_thread=threading.enumerate()
     #活着的topic消费线程
     active_topic=[]
     for i in all_topic:
         if topics[i] in all_thread:
             active_topic.append(i)
     print(active_topic)
     writeCheckpoint(topics=active_topic)


def open_thread(topic, topics,number):
    """
     open a thread to consume topic
    :param topic:
    :param topics:
    """

    if re.search(r'.+(-log)$', topic):#是否符合规则
        if kafka.topics.has_key(topic):#kafka中是否存在
            if topic not in topics.keys():#是否已经在消费
                t = myThread(esClient=es, kafkaClient=kafka, topic=topic,number=number)
                topics[topic] = t
                writeCheckpoint(topics.keys())
                t.start()
            else:
                print('warn : kafka topic %s is being comsumed' % topic)
        else:
            print('error: kafka topic %s not exist' % topic)
    else:
        print('error: kafka topic must end with "-log"')



# 连接kafka
kafka=pykafka.KafkaClient("10.18.0.15:9193,10.18.0.19:9193")
# 连接ES
es = Elasticsearch(hosts='http://119.29.165.154', port=9200)

#存储所有消费的topic及对应的线程对象的字典
topics = {}



#从checkpoint文件读取上次保存的topic 并开启线程消费
for topic in readCheckpoint():
    if not re.search(r'\s+',topic):
         open_thread(topic=topic,topics=topics)


#从管理topic(app)中获取命令   每个应用开启一个线程去消费
consumer = kafka.topics['app'].get_simple_consumer()
for messages in consumer:
    message = messages.value
    if re.search(r'.+(-log_delete)',message):#delete
        topic = message.split('_')[0]
        delete(topic=topic,topics=topics)
    elif message == '--list':#query
        query(topics)
    else:#add
        open_thread(topic=message,topics=topics)


