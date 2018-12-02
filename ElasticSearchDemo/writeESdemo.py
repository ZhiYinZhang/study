#!/usr/bin/env python
# -*- coding:utf-8 -*-
from elasticsearch import Elasticsearch
import elasticsearch.helpers as help
import time
import random
import numpy as np

#循环写入es
es=Elasticsearch(hosts='http://10.18.0.19',port=9200,http_auth=('elastic','elastic'))
index='test'
if not es.indices.exists(index):
    map = {"mappings": {
        "log": {
            "dynamic_date_formats": ["yyyy-MM-dd HH:mm:ssZ"]
        }
    }
    }
    # 创建索引
    es.indices.create(index=index, body=map)
i=0
l=['error','debug','info','warn']
# while 1:
#     level=l[random.randrange(0,l.__len__())]
#
#     i += 1
#     body = '{"level": "%s", "logger": "org.apache.coyote.http11.Http11NioProtocol","datetime":"%s+0800", "message": "Initializing ProtocolHandler [http - nio - 8899]","serverIp": "192.168.2.3"}\n' %(level,time.strftime(
#         '%Y-%m-%d %H:%M:%S'))
#     print body

    # es.index(index=index,doc_type='log',body=body)


    # time.sleep(1)  9000000 约0.5s
    # for i in range(2000000):
    #     pass



# start=time.time()
# for i in range(2000000):
#     pass
# end=time.time()
# print end-start


