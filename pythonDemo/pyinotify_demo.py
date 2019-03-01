#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/2/25 14:30
"""
#Linux的tail/tailf命令使用了内核提供的inotify功能
# pyinotify是python基于inotify的实现
# 只能在linux中使用
"""
from pyinotify import WatchManager,Notifier,ProcessEvent,IN_DELETE,IN_CREATE,IN_MODIFY
import os

last_position = 0
#获取文件中新增的数据
def tail_file(filePath):
    global last_position

    f = open(filePath, "r")
    current_position = os.path.getsize(filePath)

    if current_position <= last_position: return

    f.seek(last_position)
    data = f.readlines()
    for i in data:
        print(i)

    last_position = current_position

    f.close()


class EventHandler(ProcessEvent):
    def process_IN_CREATE(self, event):
        print("create:", event.path, event.name)

    def process_IN_DELETE(self, event):
        print("delete:", event.path, event.name)

    def process_IN_MODIFY(self, event):
        #         print("modify:",event.path,event.name)
        tail_file(event.path)


def fsMonitor(path="."):
    wm = WatchManager()  # 创建监控组
    mask = IN_CREATE | IN_DELETE | IN_MODIFY
    wm.add_watch(path, mask, auto_add=True, rec=True)# 将具体路径的监控加入监视组
    notifier = Notifier(wm, EventHandler())# 创建事件处理器，参数为监视组和对应的事件处理函数
    print("now starting monitor %s." % path)
    while True:
        try:
            notifier.process_events()  # 对事件队列中的事件逐个调用事件处理函数
            if notifier.check_events():  # 等待 检查是否有新事件到来
                print("check event true")
                notifier.read_events()  # 将新事件读入事件队列
        except KeyboardInterrupt:
            print("keyboard interrupt")
            notifier.stop()
            break

