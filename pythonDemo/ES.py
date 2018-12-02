#!/usr/bin/env python
# -*- coding:utf-8 -*-
from pykafka import KafkaClient
import re
import thread
from elasticsearch import Elasticsearch

class kafkaConsumer():
    def __init__(self):
        #连接ES
        self.es =Elasticsearch(hosts='http://119.29.165.154',port=9200)
        #连接kafka
        self.client = KafkaClient('119.29.165.154:9092')
        #获取kafka所有topic
        all_topics = self.client.topics
        #存储应用topic 过滤出以‘-log’结尾的应用日志topic    dueros-skill-log
        self.topics = []
        for topic in all_topics:
            if re.search(r'.+(-log)$', topic):
                self.topics.append(topic)

    def multiThread(self):
        for topic in self.topics:
            thread.start_new_thread(self.consumer,(topic,))
        while 1:
            pass

    def consumer(self,topic):
        app=re.split(r'-log',topic)[0]
        if not self.es.indices.exists(index=app):
            body = {"mappings": {
                "log": {
                    "properties": {
                        "datetime": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
                        "serverIp":{"type":"ip"}
                    }
                }
            }
            }
            self.es.indices.create(index=app,body=body)
        #获取消费者
        consumer=self.client.topics[topic].get_simple_consumer(auto_commit_enable=True, auto_commit_interval_ms=1000)
        self.es.indices
        #获取消费的消息
        for message in consumer:
            print(topic+'.............')
            log=message.value
            print (log)
            try:
               body=eval(log)
               self.es.index(index=app, doc_type='log', body=body)
            except:
                print('解析错误')
            #'{"level": "INFO", "logger": "org.apache.coyote.http11.Http11NioProtocol","datetime": "2018-08-15 15:19:08", "message": "Initializing ProtocolHandler ["http - nio - 8899"]","serverIp": "192.168.2.3"}'
                self.es.index(index=app,doc_type='log',body=body)

kafkaConsumer().multiThread()
