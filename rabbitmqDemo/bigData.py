#!/usr/bin/env python
# -*- coding:utf-8 -*-
# from lib.utils import parser
import json
import os
import pika


user = "bigdata"
passwd = "bigdata"
exchange="conn_test"
virtual_host="bigdata_test"
port=5672
host="139.199.161.144"
route_key="test.json.input"
# 收到消息后的回调函数
# TODO 更改消费模式，消费任务后返回ack，确保消息不丢失
def callback(ch, method, properties, body):
    print(type(body))
    print(body)


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
                             # durable=True
                             )
    #创建一个随机队列，并启用exchange
    result = channel.queue_declare(exclusive=True)

    #获取队列名
    queue_name = result.method.queue
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
