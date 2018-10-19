#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
import matplotlib.pyplot as plt
from pylab import *
mpl.rcParams['font.sans-serif']=['SimHei']

with open("E://test//rate//data//paint3//batchId.txt") as file_object1:
    contents1=file_object1.read()
batchId=contents1.split("\n")
x=range(len(batchId))
with open("E://test//rate//data//paint3//processRate.txt") as file_object2:
    processRate=file_object2.read()
y=processRate.split("\n")
with open("E://test//rate//data//paint3//inputRate.txt") as file_object3:
    inputRate=file_object3.read()
y1=inputRate.split("\n")
"""
names=['5','10','15','20','25']
x=range(len(names))
y=[0.855,0.84,0.835,0.815,0.81]
"""

#plt.plot(x,y,marker='o',mec='r',mfc='w',label=u'y=x^2曲线图')
# plt.figure(figsize=(10,8))
plt.plot(x,y,marker='.',mec='b',mfc='w',label=u'process')
plt.plot(x,y1,marker='.',mec='y',mfc='w',label=u'input')
plt.legend()

plt.xticks(x,batchId,rotation=60)
plt.margins(0)
plt.subplots_adjust(bottom=0.05)
plt.xlabel(u"batchId") #x轴标签
plt.ylabel("Rate") #y轴标签
plt.title("Memory:15G  Cores:8  Executor:1") #标题
#plt.show()
plt.savefig("e://test//rate//result//paint3.png")
