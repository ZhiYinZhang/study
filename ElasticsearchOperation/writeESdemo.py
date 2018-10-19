#!/usr/bin/env python
# -*- coding:utf-8 -*-
from elasticsearch import Elasticsearch
import time
import random
from elasticsearch import helpers

# 循环写入es
es = Elasticsearch(hosts='http://10.18.0.19', port=9200)
index = 'test123'
if not es.indices.exists(index):
    map = {"mappings": {
        "log": {
            "dynamic_date_formats": ["yyyy-MM-dd HH:mm:ssZ"]
        }
    }
    }
    # 创建索引
    es.indices.create(index=index, body=map)
i = 0
l = ['error', 'debug', 'info', 'warn']
actions = []
while 1:
    level = l[random.randrange(0, l.__len__())]

    i += 1
    body = '{"level": "%s", "logger": "org.apache.coyote.http11.Http11NioProtocol","datetime":"%s+0800", "message": "Initializing ProtocolHandler [http - nio - 8899]","serverIp": "192.168.2.3"}' % (
    level, time.strftime(
        '%Y-%m-%d %H:%M:%S'))
    print (body)

    action = {'_op_type': 'index', '_index': index, '_type': 'log', '_source': body}
    actions.append(action)
    if len(actions) == 500:
        helpers.bulk(es, actions)
        del (actions[:])
        # time.sleep(1)  9000000 约0.5s
        # for i in range(2000000):
        #     pass



        # start=time.time()
        # for i in range(2000000):
        #     pass
        # end=time.time()
        # print end-start