#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/9 14:11
import difflib
import threading

a = 'Whirlpools in the Stream with Jayesh Lalwani'
b = 'Whirlpools in the Stream'

r = difflib.SequenceMatcher(None,a,b)
print(r.quick_ratio())
print(r.get_matching_blocks())



