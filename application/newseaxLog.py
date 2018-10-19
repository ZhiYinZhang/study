#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
import time
from pykafka import KafkaClient
import re
import threading
import thread
import sys
from elasticsearch import Elasticsearch

class kafkaConsumer():
    def __init__(self, basedir=os.getcwd()):
        self.es =Elasticsearch(hosts='http://10.18.0.19',port=9200,http_auth=('elastic','elastic'))

        self.basedir = re.split(r'/$',basedir)[0]
        print(basedir)
        self.client = KafkaClient('119.29.165.154:9092')
        all_topics = self.client.topics
        #存储应用topic 过滤出以‘-log’结尾的应用日志topic
        self.topics = []
        for topic in all_topics:
            if re.search(r'.+(-log)$', topic):
                self.topics.append(topic)
        self.topics_path = self.getPath(self.topics)
    #根据topic创建每个应用日志的存储路径
    def getPath(self, topics):
        if len(self.topics) != 0:
            topic_path = {}
            for topic in topics:
                app = re.split(r'(-log)$', topic)[0]
                path = '%s/%s' % (self.basedir, app)
                if not os.path.exists(path):
                    os.makedirs(path)
                topic_path[topic] = path
            print (topic_path)
            return topic_path
        else:
            print( 'kafka topic is empty')

    # def writeToFile(self, topics):

    def writeFile(self):
        for topic in self.topics:
            # topic='dueros-skill-log'
            # self.consumer(topic)
            thread.start_new_thread(self.consumer,(topic,))
        while 1:
            pass

    def consumer(self,topic):
        # es = Elasticsearch(hosts='http://10.18.0.19', port=9200, http_auth=('elastic', 'elastic'))
        for message in self.client.topics[topic].get_simple_consumer(auto_commit_enable=True,auto_commit_interval_ms=1000):
            # basedir\appName\appName_date_logLevel.log
            file_info = '%s/%s_%s_info.log' % (
                self.topics_path[topic], re.split(r'(-log)$', topic)[0], time.strftime('%Y%m%d%H%M', time.localtime()))
            file_error = '%s/%s_%s_error.log' % (
                self.topics_path[topic], re.split(r'(-log)$', topic)[0], time.strftime('%Y%m%d%H%M', time.localtime()))
            #解析每条日志的日志级别
            level = re.split(r':|,', message.value)[1]
            print( topic+'.............'+level)
            log=message.value


            try:
               body=eval(log)
               # self.es.index(index=re.split(r'-log', topic)[0], doc_type='log', body=body)
            except:
                print ('解析错误')
                #'{"level": "INFO", "logger": "org.apache.coyote.http11.Http11NioProtocol","datetime": "2018-08-15 15:19:08", "message": "Initializing ProtocolHandler ["http - nio - 8899"]","serverIp": "192.168.2.3"}'
                # self.es.index(index=re.split(r'-log',topic)[0],doc_type='log',body=body)
            print (body)

            # print type(message.value),message.value



            # if '"ERROR"'.__eq__(level):
            #     with open(file_error, 'a') as file:
            #         file.write(message.value + '\n')
            # else:
            #     with open(file_info, 'a') as file:
            #         file.write(message.value + '\n')

# basedir=sys.argv[1]
kafkaConsumer('e:/test/t').writeFile()
# myThread('e:\\test\\t','dueros-skill-log').start()