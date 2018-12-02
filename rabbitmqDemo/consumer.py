#-*- coding: utf-8 -*-
# @Time    : 2018/9/17 11:28
# @Author  : Z
# @Email   : S
# @File    : consumer.py
import pika

def callback(ch, method, properties, body):
    print(body)

def consume():
    # rabbitmq的配置
    user = "cloudai"
    passwd = "cloudai"
    host = "139.199.161.144"
    port = 5672
    exchange = "sub_clean_result"
    virtual_host = "cloudai"
    route_key = "data_clean"
    queue_name = "sub_clean_result"

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
    channel.exchange_declare(exchange=exchange,
                             exchange_type="topic",
                             durable=True)
    channel.queue_declare(queue=queue_name)
    # 绑定
    channel.queue_bind(exchange=exchange,
                       queue=queue_name,
                       routing_key=route_key)

    channel.basic_consume(consumer_callback=callback,
                          queue=queue_name,
                          no_ack=True)
    channel.start_consuming()