#-*- coding: utf-8 -*-
# @Time    : 2018/8/12 11:02
# @Author  : Z
# @Email   : S
# @File    : demo1.py
import pykafka
import os
import sys
import re
class consumer():

    def __init__(self,basedir=os.getcwd()):
        self.basedir=basedir
        print(basedir)
        client=pykafka.KafkaClient('119.29.165.154:9092')
        all_topics=client.topics
        self.topics=[]
        for topic in all_topics:
            if re.search(r'.+(-log)$',topic):
                self.topics.append(topic)
        self.topics.path=self.getPath(self.topics)
    def getPath(self, topics):
        if len(self.topics)!=0:
            topic_path = {}
            for topic in topics:
                app=re.split(r'(-log)$', topic)[0]
                path = '%s\\%s' % (self.basedir,app)
                if not os.path.exists(path):
                    os.makedirs(path)
                topic_path[topic] = path
            print (topic_path)
            return topic_path
        else:
            print ('kafka topic is empty')

    def writeToFile(self,topics):
            print()





# consumer().writeToFile()
topic_path=consumer('E:\javacode\log').writeToFile(['skill-demo-log', 'skill-log', 'dueros-skill-log'])
print (topic_path)
# consumer('e:\\javacode\\log')