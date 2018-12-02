#-*- coding: utf-8 -*-
# @Time    : 2018/8/19 14:29
# @Author  : Z
# @Email   : S
# @File    : kafkaConsumer.py

import os
import time
from pykafka import KafkaClient
import re
import threading
import thread
import sys
class myThread(threading.Thread):
    def __init__(self,basedir,topic):
        threading.Thread.__init__(self)
        self.basedir=basedir
        self.topic=topic
    def run(self):
        kafkaConsumer(self.basedir).consumer(topic=self.topic)


class kafkaConsumer():
    def __init__(self, basedir=os.getcwd()):
        self.basedir =re.split(r'/$',basedir)[0]
        print(basedir)
        self.client = KafkaClient('119.29.165.154:9092')

        all_topics = self.client.topics
        self.topics = []
        for topic in all_topics:
            if re.search(r'.+(-log)$',str(topic)):
                self.topics.append(topic)
        self.topics_path = self.getPath(self.topics)

    def getPath(self, topics):
        if len(self.topics) != 0:
            topic_path = {}
            for topic in topics:
                app = re.split(r'(-log)$', topic)[0]
                path = '%s/%s' % (self.basedir, app)
                if not os.path.exists(path):
                    os.makedirs(path)
                topic_path[topic] = path
            print(topic_path)
            return topic_path
        else:
            print('kafka topic is empty')

    def writeFile(self):
        for topic in self.topics:
           thread.start_new_thread(self.consumer,(topic,))
        while 1:
            pass

    def consumer(self,topic):
        for message in self.client.topics[topic].get_simple_consumer(auto_commit_enable=True,auto_commit_interval_ms=1000):
            #sedir\appName\appName_date_logLevel.log
            file_info = '%s/%s_%s_info.log' % (
                self.topics_path[topic], re.split(r'(-log)$', topic)[0], time.strftime('%Y%m%d%H%M', time.localtime()))
            file_error = '%s/%s_%s_error.log' % (
                self.topics_path[topic], re.split(r'(-log)$', topic)[0], time.strftime('%Y%m%d%H%M', time.localtime()))

            level = re.split(r':|,', message.value)[1]
            print(topic+'.............'+level)
            if '"ERROR"'.__eq__(level):
                with open(file_error, 'a') as file:
                    file.write(message.value + '\n')
            else:
                with open(file_info, 'a') as file:
                    file.write(message.value + '\n')

basedir='E:/javacode/log'
kafkaConsumer(basedir).writeFile()