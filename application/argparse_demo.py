#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/11/13 16:32
import argparse
def query():
    print('query')
def add():
    print('add')
def delete():
    print("delete")

parser = argparse.ArgumentParser()

group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--list','-l',dest='command',nargs='?',const='query',help='列出所有正在消费的kafka topic')
group.add_argument('--add','-a',dest='command',nargs='?',const='add',help="添加一个topic去消费")
group.add_argument('--delete','-d',dest='command',nargs='?',const='delete',help='从正在消费的topics中移除')

parser.add_argument('--topic','-t',type=str,nargs='?',help="kafka topic name")

add = parser.add_argument_group("optional of add operate")
add.add_argument('--batch','-b',type=int,nargs='?',const=200,default=200,help="写入到Elasticsearch每个批次的大小，batch越大，速度越快，实时性越低，默认即可,default:200")
add.add_argument('--retain','-r',type=int,nargs='?',const=2,default=2,help="删除Elasticsearch过期index时,保留多少个(包括正在写的),default:2")
add.add_argument('--interval','-i',type=int,nargs='?',const=1,default=1,help="Elasticsearch每隔多久生成topic的一个新的index,即日志每隔多久rollover一次,default:1")
add.add_argument('--unit','-u',type=str,choices=('d','h','m','s'),nargs='?',const='s',default='s',help="设置interval单位,d:day h:hour m:minutes s:second,default:d")


args = parser.parse_args()

print(args,type(args.topic),type(args))

if args.topic:
    print(args.topic)
else:
    print('exception')

