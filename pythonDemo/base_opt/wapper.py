#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/12/19 14:13
from datetime import datetime as dt

#装饰函数a，接收一个函数，a使用一个函数c去装饰传入的函数，并返回函数c
def wrapper(a_func):
    def wrap_func():
        print(f"start:{dt.now()}")
        a_func()
        print(f"end:{dt.now()}")


    return wrap_func

@wrapper
def is_wrap_func():
    print(dt.now())

is_wrap_func()

"""
这两条命令等同于 在is_wrap_func函数上定义一个@wrapper
1.将is_wrap_func传给wrapper函数
2.wrapper返回一个函数，并调用这个函数
"""
# a=wrapper(is_wrap_func)
# a()



