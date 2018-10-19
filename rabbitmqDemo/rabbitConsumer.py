#!/usr/bin/env python
# -*- coding:utf-8 -*-
# from lib.utils import parser
import json
import os
import pika

user="cloudai"
passwd="cloudai"
host="139.199.161.144"
port=5672
virtual_host="cloudai"
# exchange="pub_data_clean"
# queue_name='pub_data_clean'
# exchange="sub_clean_result"
# queue_name="sub_clean_result"
# route_key='data_clean'
exchange = "data_result"
queue_name = "data_result"
route_key = "data_result"


# 收到消息后的回调函数
# TODO 更改消费模式，消费任务后返回ack，确保消息不丢失
def callback(ch, method, properties, body):
    s=str(body)

    d=json.loads(body)
    # l=d["data"]["2"]
    print(d)


def consume():
    # 配置消费者
    # TODO 更改配置方式，写入配置文件
    credential = pika.PlainCredentials(user, passwd)
    # 创建RabbitMQ连接
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host,
                                                                   credentials=credential,
                                                                   virtual_host=virtual_host,
                                                                   port=port
                                                                   ))
    # 初始化channel
    channel = connection.channel()
    #定义一个exchange 类型为topic    如果exchange设置了持久化  durable设为True
    channel.exchange_declare(exchange=exchange,
                             exchange_type="topic",
                             durable=True
                             )
    #创建一个随机队列，并启用exchange
    # result = channel.queue_declare()
    # channel.queue_declare(exclusive=True)
    channel.queue_declare(queue=queue_name)
    #获取队列名
    # queue_name = result.method.queue
    print ("queue name",queue_name)
    # 绑定topic，按照test.json.input key消费消息
    channel.queue_bind(exchange=exchange,
                        queue=queue_name,
                        routing_key=route_key
                       )
    # channel.exchange_bind(
    #                    queue=queue_name,
    #                    # routing_key="test.json.input"
    #                    )

    channel.basic_consume(consumer_callback=callback,
                          queue=queue_name,
                          no_ack=True)
    channel.start_consuming()


if __name__ == '__main__':
    consume()
