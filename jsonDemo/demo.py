#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/1/10 16:06
import json

path="e://test//json//shaoshanshi.json"
with open(path,"r",encoding="utf-8") as reader:
    m=json.load(reader)

m.append({"c1":"中文","c2":"4"})


with open(path,"w",encoding="utf=8") as writer:
    json.dump(m,writer,ensure_ascii=False)