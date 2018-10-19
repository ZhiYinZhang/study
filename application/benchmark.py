#!/usr/bin/env python
# -*- coding:utf-8 -*-
from elasticsearch import Elasticsearch
import time
import sys
es=Elasticsearch(hosts='http://10.18.0.19',port=9200,http_auth=('elastic','elastic'))
total=int(sys.argv[1])
if not es.indices.exists(index='benchmark'):
    body = {"mappings": {
        "log": {
            "dynamic_date_formats": ["yyyy-MM-dd HH:mm:ssZ"]
        }
    }
    }
    es.indices.create(index='benchmark',body=body)

start=time.time()

for i in range(0,total):
       body='{"level": "INFO", "logger": "org.apache.coyote.http11.Http11NioProtocol","datetime":"%s+0800", "message": "Initializing ProtocolHandler ","serverIp": "192.168.2.3","range":%s}'%(time.strftime('%Y-%m-%d %H:%M:%S'),str(i))
       es.index(index='benchmark',doc_type='doc',body=body)


end=time.time()
print('%s-%s=%s s'%(end,start,end-start))