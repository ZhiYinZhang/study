#!/usr/bin/env python
# -*- coding:utf-8 -*-
import time
import random
from elasticsearch import Elasticsearch
from elasticsearch import helpers
es=Elasticsearch(hosts='http://10.18.0.19',port=9200,http_auth=('elastic','elastic'))
index='bulk1'
if not es.indices.exists(index):
    map = {"mappings": {
        "log": {
            "dynamic_date_formats": ["yyyy-MM-dd HH:mm:ssZ"]
        }
    }
    }
    # 创建索引
    es.indices.create(index=index, body=map)

bodys=[]
l=['error','debug','info','warn']


# start1=time.time()
# for i in range(10000):
#     level=l[random.randrange(0,l.__len__())]
#     body = '{"level": "%s", "logger": "org.apache.coyote.http11.Http11NioProtocol","datetime":"%s+0800", "message": "Initializing ProtocolHandler [http - nio - 8899]","serverIp": "192.168.2.3"}\n' %(level,time.strftime(
#             '%Y-%m-%d %H:%M:%S'))
#     es.index(index=index,doc_type='log',body=body)
# end1=time.time()
# print 'index:',end1-start1


s=time.time()
for n in range(10000):
    level=l[random.randrange(0,l.__len__())]
    body = '{"level": "%s", "logger": "org.apache.coyote.http11.Http11NioProtocol","datetime":"%s+0800", "message": "Initializing ProtocolHandler [http - nio - 8899]","serverIp": "192.168.2.3"}' %(level,time.strftime(
            '%Y-%m-%d %H:%M:%S'))
    bodys.append(body)

body1 = '{"level": "%s", "logger": "org.apache.coyote.http11.Http11NioProtocol","datetime":"%s+0800", "message": "Initializing ProtocolHandler ["http - nio - 8899"]","serverIp": "192.168.2.3"}' %(l[1],time.strftime(
            '%Y-%m-%d %H:%M:%S'))
body2 = '{"level": "%s", "logger": "org.apache.coyote.http11.Http11NioProtocol","datetime":"%s+0800", "message": "Initializing ProtocolHandler [http - nio - 8899]","serverIp": "192.168.2.3"}' %(l[0],time.strftime(
            '%Y-%m-%d %H:%M:%S'))
bodys.append(body1)
bodys.append(body2)
actions=[{'_op_type':'index',
          '_index':index,
          '_type':'log',
          '_source':b}
         for b in bodys]

##bulk
start2=time.time()
helpers.bulk(client=es,actions=actions)
end2=time.time()
print ('bulk:',end2-start2)

##streaming_bulk
# start3=time.time()
# for ok,response in helpers.streaming_bulk(client=es,actions=actions,chunk_size=2000):
#     if not ok:
#         #failure inserting
#         print response
# end3=time.time()
# print 'streaming_bulk',end3-start3



# start4=time.time()
# for o,res in helpers.parallel_bulk(client=es,actions=actions):
#     if not o:
#         print res
# end4=time.time()
# print 'parallel_bulk',end4-start4