#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/1/17 14:15
import time
import os
from watchdog.events import FileSystemEventHandler,FileCreatedEvent,FileModifiedEvent
from watchdog.observers import Observer

class FileMonitorHandler(FileSystemEventHandler):
    def __init__(self,**kwargs):
        super(FileMonitorHandler,self).__init__(**kwargs)
    def on_modified(self, event:FileModifiedEvent):
        print(event.src_path,event.event_type)
        tail_file(event.src_path)
    def on_created(self, event:FileCreatedEvent):
        print(event.src_path, event.event_type)
        global last_position
        last_position=0

last_position=0
def tail_file(file_path):
    global last_position
    current_position=os.path.getsize(file_path)
    #上一次偏移量 大于等于 当前偏移量
    if last_position>=current_position: return

    time.sleep(5)
    with open(file_path,"r") as reader:
          reader.seek(last_position)
          data=reader.readlines()

          file_name=os.path.basename(file_path)
          with open(f"e://test//log/files_bak/{file_name}","w+") as writer:
              writer.writelines(data)
    last_position=current_position
    print(file_name,last_position)

if __name__=="__main__":
    path="e://test//log/files"
    # event_handler=FileMonitorHandler()
    # observer=Observer()
    # observer.schedule(event_handler,path,recursive=True)
    # observer.start()
    # observer.join()

