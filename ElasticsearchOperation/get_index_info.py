#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/11/19 10:11
from elasticsearch import Elasticsearch
import time
import json
from datetime import datetime
format_map = {
    "d":"%Y%m%d",
    "h":"%Y%m%d%H",
    "m":"%Y%m%d%H%M",
    "s":"%Y%m%d%H%M%S"
}
es = Elasticsearch(hosts="http://119.29.165.154",post=9200)
result:dict = es.indices.get_settings(index="test1-log_2018111914")
start = time.time()
for i in result.values():
    t = int(i['settings']['index']['creation_date'])/1000

with open("../application/checkpoint/test1-log.json",'r') as file:
    param = json.load(file)


s = datetime.fromtimestamp(t).strftime(format_map['h'])

s1 = datetime.strptime(s,format_map['h'])

t1 = s1.timestamp()
print(t)
print(s)
print(s1)
print(t1)

