#!/usr/bin/env python
# -*- coding:utf-8 -*-
import pika
import json


res_data = {
        "data": {"1": [
            {
                "batchId": 15,
                "handleTimestamp": "2018-06-14T10:38:32.583+08:00",
                "num": 17000000,
                "timestamp": 1528943910523
            },
            {
                "batchId": 15,
                "handleTimestamp": "2018-06-14T10:38:32.583+08:00",
                "num": 17000001,
                "timestamp": 1528943910523
            },
            {
                "batchId": 15,
                "handleTimestamp": "2018-06-14T10:38:32.583+08:00",
                "num": 17000002,
                "timestamp": 1528943910523
            },
            {
                "batchId": 15,
                "handleTimestamp": "2018-06-14T10:38:32.583+08:00",
                "num": 17000003,
                "timestamp": 1528943910523
            },
            {
                "batchId": 15,
                "handleTimestamp": "2018-06-14T10:38:32.583+08:00",
                "num": 17000004,
                "timestamp": 1528943910523
            },
            {
                "batchId": 15,
                "handleTimestamp": "2018-06-14T10:38:32.583+08:00",
                "num": 17000005,
                "timestamp": 1528943910523
            },
            {
                "batchId": 15,
                "handleTimestamp": "2018-06-14T10:38:32.583+08:00",
                "num": 17000006,
                "timestamp": 1528943910523
            },
            {
                "batchId": 15,
                "handleTimestamp": "2018-06-14T10:38:32.583+08:00",
                "num": 17000007,
                "timestamp": 1528943910523
            },
            {
                "batchId": 15,
                "handleTimestamp": "2018-06-14T10:38:32.583+08:00",
                "num": 17000008,
                "timestamp": 1528943910523
            },
            {
                "batchId": 15,
                "handleTimestamp": "2018-06-14T10:38:32.583+08:00",
                "num": 17000009,
                "timestamp": 1528943910523
            }
        ]
        },
        "userId": "231241",
        "sessionId":"dfasjdifsajoerjqwkrejq"
    }



def produce(data :dict):
    user = "cloudai"
    passwd = "cloudai"
    host = "139.199.161.144"
    port = 5672
    virtual_host = "cloudai"
    exchange="sub_clean_result"
    queue_name="sub_clean_result"
    # exchange = "pub_data_clean"
    # queue_name = "pub_data_clean"
    route_key = "data_clean"

    credential = pika.PlainCredentials(user, passwd)
    #链接rabbit服务器（localhost是本机，如果是其他服务器请修改为ip地址）
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host,
                                                                   port=port,
                                                                   virtual_host=virtual_host,
                                                                   credentials=credential))
    #初始化channel
    channel = connection.channel()
    #如果exchange设置了持久化  durable设为True
    channel.exchange_declare(exchange=exchange,exchange_type="topic", durable=True)
    channel.queue_declare(queue=queue_name)
    channel.queue_bind(exchange=exchange,
                       queue=queue_name,
                       routing_key=route_key
                       )


    msg_props=pika.BasicProperties()
    msg_props.content_type="text/plain"
    #向队列插入数值 routing_key是队列名 body是要插入的内容
    channel.basic_publish(exchange=exchange,
                      routing_key=route_key,
                      properties=msg_props,
                      body=json.dumps(data))
    print("开始队列")
    #缓冲区已经flush而且消息已经确认发送到了RabbitMQ中，关闭链接
    connection.close()

if __name__=="__main__":
    produce(res_data)