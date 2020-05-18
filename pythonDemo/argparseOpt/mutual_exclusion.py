#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/5/18 16:14
import argparse
"""
创建一个互斥的组，组内参数不能同时使用
"""
if __name__=="__main__":
    parser = argparse.ArgumentParser()

    group=parser.add_mutually_exclusive_group()

    group.add_argument("--start",help="start command",const="start",nargs="?")
    group.add_argument("--stop",help="stop command",const="stop",nargs="?")
    group.add_argument("--status",help="status command",const="status",nargs="?")

    args=parser.parse_args()
    print(args)