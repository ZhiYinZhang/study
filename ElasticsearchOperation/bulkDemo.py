#!/usr/bin/env python
# -*- coding:utf-8 -*-
import time
import random
from elasticsearch import Elasticsearch
from elasticsearch import helpers
es=Elasticsearch(hosts='http://10.18.0.19',port=9200,http_auth=('elastic','elastic'))
index='log_level'
if not es.indices.exists(index):
    map = {"mappings": {
        "doc": {
            "dynamic_date_formats": ["yyyy-MM-dd HH:mm:ssZ"]
        }
    }
    }
    # 创建索引
    es.indices.create(index=index, body=map)



es=Elasticsearch(hosts='http://localhost',port=9200)
levels=['info','debug','warn','error']
actions=[]
for i in range(100):
    level=levels[random.randrange(0,len(levels))]
    action={'_op_type':'index',#操作 index update create delete
            '_index':'log_level',#index
            '_type':'doc',  #type
            '_source':{'level':level}}
    actions.append(action)
helpers.bulk(client=es,actions=actions)

helpers.parallel_bulk(client=es,)