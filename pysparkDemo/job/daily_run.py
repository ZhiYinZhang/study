#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/5/13 10:58
from datetime import datetime as dt
import time


while True:
    print(str(dt.now()))

    time.sleep(1*60*60)

    date=dt.now()

    day=date.day
    weekday=date.isoweekday()
    hour=date.hour

    if day==1:
        if hour==4:
            print(f"{str(dt.now())}-----monthly run")

    if weekday==1:
        if hour==4:
            print(f"{str(dt.now())}-----weekly run")

    if hour==4:
        print(f"{str(dt.now())}-----daily run")
