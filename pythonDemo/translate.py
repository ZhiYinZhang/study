#-*- coding: utf-8 -*-
# @Time    : 2018/3/25 14:50
# @Author  : Z
# @Email   : S

from translate import Translator
import time
import traceback as tb
if __name__=="__main__":
    t = Translator(to_lang="zh")

    with open("E:\资料\Databricks\en.txt",'r') as reader:

        en = reader.readlines()

    for i in en:
        i = i[:-2]
        try:
            zh = t.translate(i)
            print(zh)
        except:
            print("失败:",i)
            tb.print_exc()
