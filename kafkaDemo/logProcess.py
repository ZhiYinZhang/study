#!/usr/bin/env python
# -*- coding:utf-8 -*-
import pykafka
import logging
class KafkaHandler(logging.Handler):
    def __init__(self,kafka_hosts,topic):
        logging.Handler.__init__(self)
        self.client=pykafka.KafkaClient(kafka_hosts)
        self.topic=self.client.topics[topic]
        self.producer = self.topic.get_producer()
    def emit(self, record):
            msg=self.format(record)
            print(msg)
            self.producer.produce(msg)
    def close(self):
         self.producer.stop()

class mylog():
    def __init__(self):
        self.mylog = logging.getLogger('mylog')
        self.mylog.setLevel(level=logging.DEBUG)
        self.kh = KafkaHandler('10.18.0.15:9193,10.18.0.19:9193,10.18.0.26:9193', 'rate')
        self.kh.setFormatter(logging.Formatter(
            '%(asctime)s %(user)s[%(sessionId)s] %(pathname)s[line %(lineno)s]-%(levelname)s:\n %(message)s'))
        self.mylog.addHandler(self.kh)
    def info(self,message,user,sessionId):
        self.mylog.info(message,extra={'user':user,'sessionId':sessionId})

    def warning(self,message,user,sessionId):
        self.mylog.warning(message,extra={'user':user,'sessionId':sessionId})

    def error(self,message,user,sessionId):
        self.mylog.error(message,exc_info=True,extra={'user':user,'sessionId':sessionId})