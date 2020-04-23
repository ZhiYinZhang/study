#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/1/10 9:54
import json

obj1={"a":1,"b":2}
obj2=[1,2,3,4]
obj3=[{"a":1,"b":2},{"a":3,"b":4}]

#1.将对象转成字符串
# json_str=json.dumps(obj3)
# print(type(json_str),json_str)


path="e://test//json/test.json"

#2.将对象写入文件
# file_writer=open(path,mode="w")
# json.dump(obj3,file_writer)


#3.将字符串转成对象
# json_str1='{"a":1,"b":2}'
# json_str2='[1,2,3]'
# json_str3='[{"a":1,"b":2},{"a":3,"b":4}]'
#
#
# obj4=json.loads(json_str3)
# print(type(obj4),obj4)

#4.将文件转成对象
file_reader=open(path,mode="r")
obj5=json.load(file_reader)

print(type(obj5),obj5)