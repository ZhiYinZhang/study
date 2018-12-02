#!/usr/bin/env python
# -*- coding:utf-8 -*-
import pykafka
from elasticsearch import client
from elasticsearch import Elasticsearch
import time
import datetime
import threading
import re
class myThread(threading.Thread):
    def __init__(self,esClient,kafkaClient,topic):
        threading.Thread.__init__(self)
        self.esClient=esClient
        self.kafkaClient=kafkaClient
        self.topic=topic
    def run(self):
        print ("start.................%s"%self.topic)
        kafkaConsumer(self.esClient, self.kafkaClient, self.topic)
        print ("exit..................%s"%self.topic)
def printTime(threadName):
    for i in range(1000):
        time.sleep(1)
        print ("%s---%s"%(threadName,datetime.datetime.now()))


def kafkaConsumer(esClient, kafkaClient, topic):
        print('start write %s'%topic)
        # 获取消费者
        consumer = kafkaClient.topics[topic].get_simple_consumer()
        # 切割 ‘appName-log’  拼接日期    appName_年月日时分
        app=re.split(r'-log', topic)[0]
        index='test123'
        # createIndex(esClient=esClient,index=index)
        # 获取消费的消息
        for message in consumer:

            # index = updateIndex(app, esClient=esClient)
            log = message.value
            print (log)
            try:
                body = eval(log)
                esClient.index(index=index, doc_type='log', body=body)
            except:
                 try:
                 # '{"level": "INFO", "logger": "org.apache.coyote.http11.Http11NioProtocol","datetime": "2018-08-15 15:19:08+0800", "message": "Initializing ProtocolHandler ["http - nio - 8899"]","serverIp": "192.168.2.3"}'
                  log2= log.replace('["http - nio - 8899"]', '[http - nio - 8899]')
                  esClient.index(index=index, doc_type='log', body=log2)
                 except:
                    esClient.index(index=index, doc_type='log', body='{"message":"write elasticsearch error","datetime":"%s+0800"}'%time.strftime('%Y-%m-%d %H:%M:%S'))

def updateIndex(app,esClient):
    # 拼接
    now = time.strftime('%Y%m%d%H%M', time.localtime())
    index = '%s_%s' % (app, now)
    # 设置index的mapping，定义ES解析的日期格式   不存在就创建
    if not esClient.indices.exists(index=index):
        createIndex(esClient=esClient,index=index)
        # 删除过期索引
        expire_index = '%s_%s' % (app, str(int(now) - 2))
        #判断是否存在
        if esClient.indices.exists(index=expire_index):
             esClient.indices.close(index=expire_index)
             esClient.indices.delete(index=expire_index)
    return index
def createIndex(esClient,index):
    body = {"mappings": {
        "log": {
            "dynamic_date_formats": ["yyyy-MM-dd HH:mm:ssZ"]
        }
    }
    }
    # 创建索引
    esClient.indices.create(index=index, body=body)



# 连接kafka
kafka=pykafka.KafkaClient("10.18.0.15:9193,10.18.0.19:9193,10.18.0.26:9193")
# 连接ES
# es = Elasticsearch(hosts='http://119.29.165.154', port=9200)
es = Elasticsearch(hosts='http://10.18.0.19', port=9200,http_auth=('elastic','elastic'))

#从管理topic(app)中获取应用topic名称   每个应用开启一个线程取消费
consumer=kafka.topics['app'].get_simple_consumer()
for message in consumer:
    topic=message.value
    #判断是否合法
    if re.search(r".+(-log)$",topic) and kafka.topics.has_key(topic):
              myThread(esClient=es,kafkaClient=kafka,topic=topic).start()
    else:
        print('kafka topic %s is not exist' %topic )



