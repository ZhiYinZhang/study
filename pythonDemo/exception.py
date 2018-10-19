#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
import datetime
import time
import logging
import logging.handlers
import threading
class mylog():
  def __init__(self,name=None):
    self.name=name
    # 获取日志器
    self.logger=logging.getLogger(self.name)
    self.logger.setLevel(level=logging.DEBUG)



    # 根据时间滚动的文件处理器
    trfh=logging.handlers.TimedRotatingFileHandler("E:\\test\\txt\\all.log",when='M',interval=1,backupCount=10)
    trfh.setFormatter(logging.Formatter('%(asctime)s %(pathname)s[line %(lineno)s] %(process)s %(threadName)s[%(thread)s] %(levelname)s %(name)s: %(message)s'))

    # 文件处理器
    fh=logging.FileHandler('e:\\test\\txt\\error.log')
    fh.setLevel(logging.ERROR)
    fh.setFormatter(logging.Formatter('%(asctime)s %(pathname)s[line %(lineno)s] %(process)s %(threadName)s[%(thread)s] %(levelname)s %(name)s:  %(message)s'))

    self.logger.addHandler(trfh)
    self.logger.addHandler(fh)

  def info(self,message):
      self.logger.info(message,exc_info=True)
  def warning(self,message):
      self.logger.warning(message,exc_info=True)
  def error(self,message):
      self.logger.error(message,exc_info=True)



