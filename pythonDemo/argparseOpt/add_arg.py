#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/5/18 14:51
import argparse

if __name__=="__main__":
     parser=argparse.ArgumentParser(description="在参数帮助文档之前显示的文本")
     #参数需要使用--b或-b
     parser.add_argument("--by","-b",     #参数变量，‘-’表示缩写
                         action="store",  #将命令行参数与action相关联，默认store:存储参数的值
                         type=str,       #可以指定数据类型
                         help="b help",  #帮助信息
                         const="1111",   #给了-b/--b，但是-b后面没接参数值时，默认的值
                         default="2222", #没给-b/--b时，默认值，结合nargs使用
                         nargs="?",      #"?"表示消耗一个参数，没有命令行参数，会使用default
                         required=False, #该参数是否可选，为True表示必须
                         dest="bb",      #parse_args()返回的属性名，默认是和参数变量一样:by or b
                         metavar="3333",  #参数示例
                         choices=["1111","2222","3333"] #参数范围
                         )
     #按位置的参数，add_arg.py -b 1 2,a=2
     parser.add_argument("a",type=int,help="a help",default=2)


     # parser.print_help()
     args=parser.parse_args()
     print(args)