#!/usr/bin/env python
# -*- coding:utf-8 -*-
import time
import random
from elasticsearch import Elasticsearch
from elasticsearch import helpers
es = Elasticsearch(hosts='http://119.29.165.154', port=9200)
index='test3'
if not es.indices.exists(index):
    map = {"mappings": {
        "doc": {
            "dynamic_date_formats": ["yyyy-MM-dd HH:mm:ssZ"]
        }
    }
    }
    # 创建索引
    es.indices.create(index=index, body=map)


levels=['info','debug','warn','error']
actions=[]
for i in range(100):
    # time.sleep(1)
    level=levels[random.randrange(0,len(levels))]

    body = '{"level": "%s", "logger": "org.apache.coyote.http11.Http11NioProtocol","datetime":"%s", "message": "Initializing ProtocolHandler [http - nio - 8899]","serverIp": "192.168.2.3"}' % (
        level, time.strftime(
            '%Y-%m-%d %H:%M:%S'))
    action={'_op_type':'index',#操作 index update create delete
            '_index':index,#index
            '_type':'doc',  #type
            '_source':{"message":body}}
    actions.append(action)
helpers.bulk(client=es,actions=actions)

# helpers.parallel_bulk(client=es,actions=actions)