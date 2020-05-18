#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/5/18 15:52
import argparse
if __name__=="__main__":
    parser = argparse.ArgumentParser()
    group1=parser.add_argument_group("group1")
    group1.add_argument("--a",help="a help")

    group2 = parser.add_argument_group("group2")
    group2.add_argument("--b",help="b help")

    parser.print_help()

    # args=parser.parse_args()
