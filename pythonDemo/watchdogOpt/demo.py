#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/1/17 14:14
import sys
import time
import logging
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler

if __name__=="__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S'
                        )
    event_handler=LoggingEventHandler()

    observer=Observer()

    path="e://test//log/files"
    observer.schedule(event_handler,path,recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
