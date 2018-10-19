#!/usr/bin/env python
# -*- coding:utf-8 -*-
import time
import random
from elasticsearch import Elasticsearch
from elasticsearch import helpers
def getDate():
    return time.strftime("%Y%m%d%H%M%S", time.localtime())

es=Elasticsearch(hosts='http://10.18.0.19',port=9200)

# index=f'rollover_{getDate()}'
# def createIndex():

def getDate(seconds):
    # current_time=time.time()
    date= time.strftime("%Y%m%d%H%M", time.localtime(seconds))
    return int(date)
def deleteExpire(expire_date):
    expire_index=f"{topic}_{expire_date}"
    if es.indices.exists(expire_index):
        es.indices.delete(index=expire_index)


date = {}
topic='log'
curr_time=time.time()
date["start_date"]=getDate(curr_time)
date["end_date"]=getDate(curr_time+3*60)
date["expire_date"]=getDate(curr_time-6*60)
def createIndex():
    curr_time=time.time()
    curr_date=getDate(curr_time)
    if curr_date>=date["end_date"]:
        date["start_date"]=curr_date
        date["end_date"]=getDate(curr_time+3*60)
        date["expire_date"]=getDate(curr_time-6*60)
        deleteExpire(expire_date=date["expire_date"])
    new_index=f"{topic}_{date['start_date']}"
    if not es.indices.exists(new_index):
        map = {"mappings":
                    {
                        "doc": {
                            "dynamic_date_formats": ["yyyy-MM-dd HH:mm:ssZ"]
                        }
                    }
              }
        # 创建索引
        es.indices.create(index=new_index, body=map)
    return new_index

levels=['info','debug','warn','error']
actions=[]
i=0
index=createIndex()
while True:
    level=levels[random.randrange(0,len(levels))]
    action={'_op_type':'index',#操作 index update create delete
            '_index':index,#index
            '_type':'doc',  #type
            '_source':{'level':level}}
    actions.append(action)
    if(len(actions))==1000:
       helpers.bulk(client=es,actions=actions)
       del(actions[:])
       index=createIndex()
       # helpers.parallel_bulk(client=es,actions=actions)
