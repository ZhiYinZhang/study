#!/usr/bin/env python
# -*- coding:utf-8 -*-
from kazoo.client import KazooClient
from pykafka import KafkaClient
def zk():
     zk=KazooClient(hosts='10.18.0.15:3191,10.18.0.19:3191,10.18.0.26:3191')
     zk.start()
     state=zk.state
     if zk.connected:
        print ("connect success")
        # zk.create(path='/test1')
        # zk.delete(path='/test')
        # b=zk.exists(path="/test1",watch=)
        # print b
        # a=zk.get(path="/")
        b= zk.get_children("/")
if __name__=="__main__":
    zk()

