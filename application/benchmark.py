#!/usr/bin/env python
# -*- coding:utf-8 -*-
from elasticsearch import Elasticsearch
import time
from elasticsearch import helpers
import json
import sys
es = Elasticsearch(hosts='http://119.29.165.154', port=9200)
# total=int(sys.argv[1])
index = 'test-log_20181114100544'
if not es.indices.exists(index=index):
    body = {"mappings": {
        "log": {
            "dynamic_date_formats": ["yyyy-MM-dd HH:mm:ssZ"]
        }
    }
    }
    es.indices.create(index=index,body=body)

def benchmark(num):
    start=time.time()
    actions = []

    for i in range(0,10):
           body='{"level": "INFO", "logger": "org.apache.coyote.http11.Http11NioProtocol","datetime":"%s+0800", "message": "Initializing ProtocolHandler ","serverIp": "192.168.2.3","range":%s}\n'%(time.strftime('%Y-%m-%d %H:%M:%S'),str(i))
           # body = {"log":body}
           action = {'_op_type': 'index', '_index': index, '_type': 'log', '_source': body}
           actions.append(action)
           if len(actions)==num:
               result = helpers.bulk(client=es,actions=actions,stats_only=True,raise_on_error=False)
               print(result)
               del(actions[:])

    end=time.time()
    print('%s--------%s-%s=%s s'%(num,end,start,end-start))


if __name__=="__main__":
    # for i in range(100,1100,100):
    benchmark(10)
    # print(es.cat.indices())