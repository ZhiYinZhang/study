#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/11 14:59
import math as m
from datetime import date
import datetime as dt
import time
import os, sys


def week_diff(year,month,week,today_year,today_month,today_week):
          db = today_year - year
          if week==1 and month!=today_month:
               db-=1
          week_diff=(today_week+52*db)-week
          return week_diff

os.environ['PYTHON_EGG_CACHE']="e://test//python_egg_cache"
abspath=os.path.dirname(__file__)
print(abspath)
sys.path.append(abspath)

os.chdir(abspath)