#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
from numpy.random import randn
def readCheckpoint():
    """
    读取checkpoint文件中的topic
    :return: 返回从checkpoint文件中读取的topic
    """
    path='E:/test/topic.txt'
    if os.path.exists(path):
        with open(path, mode='r') as file:
            print (file.read())
    else:
        print('no such file: %s'%path)

readCheckpoint()
t=['a','b','c']
def writeCheckpoint(topics):
    """

    :param topics:
    """
    with open('E:/test/topic.txt',mode='w') as file:
        for i in topics:
            file.write(i+'\n')


writeCheckpoint(t)