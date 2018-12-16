#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/11/27 14:53
import time
import sys

temp_size = 0
total_size = 50

for i in range(51):
    # time.sleep(1)
    temp_size = i
    done = 50*temp_size//total_size
    s = 100*temp_size/total_size
    sys.stdout.write("\r[%s%s] %d%%"%('#'*done,' '*(50-done),s))
    sys.stdout.flush()
