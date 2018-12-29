#!/usr/bin/env python
# -*- coding:utf-8 -*-
import pykafka
from elasticsearch import helpers
import time
import sys
from application.newseax_log_system.utils import *




class myThread(threading.Thread):
    def __init__(self,esClient,kafkaClient:pykafka.KafkaClient,param:dict,topics:list):
        threading.Thread.__init__(self)
        self.setName(param['topic'])

        self.esClient = esClient
        self.kafkaClient = kafkaClient
        self.param = param
        self.topics = topics
        self.flag = False
    def run(self):
        print(f"start {self.param['topic']} thread")
        try:
            kafka2ES(self.esClient, self.kafkaClient, self.param,self)
        except Exception:
            delete(self.param['topic'],self.topics)
            tb.print_exc()
        print(f"exit {self.param['topic']} thread")
    #线程是否退出的标志  True:退出
    def Flag(self):
        self.flag=True

#从kafka中消费消息并写入elasticsearch
def kafka2ES(esClient, kafkaClient, param:dict,myThread):
    """

    :param esClient: elasticSearch client
    :param kafkaClient:  kafka client
    :param param:
    :param mythread:
    """
    topic = param['topic']
    if not param.get('last_index'):
        indexs = get_app_indexs(topic, esClient)
        if len(indexs) == 0:  # 首次创建,创建第一个index
            #根据unit得到topic拼接的日期格式，得到index
            last_index = get_index(topic=topic,date_format=format_map[param['unit']])
            # 创建新索引
            esClient.indices.create(index=last_index)
            #从ES中获取index创建时间戳
            last_time = get_index_createTime(esClient,last_index,param)

            param['last_index'] = last_index
            param['last_time'] = last_time
            writeCheckpoint(param)
            print(f"首次创建ES的index：{last_index}")
        else:#上次正常退出 即使用--delete
            #获取ES中该topic创建时间最近的一个index
            last_index = indexs[0]
            param['last_index'] = last_index
            last_time = get_index_createTime(esClient, last_index,param)
            param['last_time'] = last_time
            writeCheckpoint(param)
    else:#上次非正常退出,即不是通过--delete
        pass


    print('start write %s'%topic)
    # 获取消费者
    consumer = kafkaClient.topics[bytes(topic,'utf-8')].get_simple_consumer()
    # 获取消费的消息
    actions = []
    print(f"开始消费{topic}")
    for message in consumer:
            if myThread.flag:
                sys.exit()
            #创建ES的index 和 删除过期的index
            index = createIndex(param=param, esClient=esClient)

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

#----------------------------------------------调度----------------------------------------------------------------


#开启一个线程去消费topic
def open_thread(param, topics):
    topic = param['topic']
    if re.search(r'.+(-log)$', topic):#是否符合规则
        if bytes(topic,'utf-8') in kafka.topics.keys():#kafka中是否存在
            if topic not in topics.keys():#是否已经在消费
                thread_obj = myThread(esClient=es, kafkaClient=kafka, param=param,topics=topics)
                topics[topic] = thread_obj
                writeCheckpoint(param)
                thread_obj.start()
            else:
                print('warn : kafka topic %s is being comsumed' % topic)
        else:
            print('error: kafka topic %s not exist' % topic)
    else:
        print('error: kafka topic must end with "-log"')



if __name__=="__main__":
    # 连接kafka
    kafka=pykafka.KafkaClient("10.18.0.11:9092,10.18.0.8:9092")
    # 连接ES
    es = Elasticsearch(hosts='http://119.29.165.154', port=9200)

    manager_topic = b'app'
    #存储所有消费的topic及对应的线程对象的字典
    topics = {}

    #从checkpoint文件读取上次保存的topic 并开启线程消费
    for i in readCheckpoint():
         print("开启上一次")
         open_thread(param=i,topics=topics)

    #从管理topic(app)中获取应用topic名称   每个应用开启一个线程取消费
    consumer=kafka.topics[manager_topic].get_simple_consumer()
    for messages in consumer:
        time.sleep(1)
        message=messages.value.decode()
        try:
          message:dict = json.loads(message)
          print(f"message:{message}")

          command = message['command']
          if command== 'delete':
              topic = message['topic']
              delete(topic=topic,topics=topics)
          elif command == 'list':
              query()
          elif command == 'add':
              open_thread(param=message,topics=topics)
        except Exception:
            # tb.print_exc()
            print(message)
