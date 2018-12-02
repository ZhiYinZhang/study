#-*- coding: utf-8 -*-
# @Time    : 2018/3/28 15:18
# @Author  : Z
# @Email   : S
# @File    : queryOneWord.py



#各个字符串中第一个只出现了一次的字符
strList=['bacddc','0106218286','x1k0yxzk1','acebadcedbf']
str1=['','','','']#用来存储第一个字符串中出现一次的字符
i=0
for str in strList:#遍历字符串数组
        for chr in str:#遍历字符串
            if str.count(chr)==1 :#判断每个字符出现次数是否为1次
                    str1[i]=chr#如果是就添加到str1中
                    break
        i+=1

#遍历打印
j=1
for s in str1:
   print ('第%d个字符串第一个出现一次的字符：'%j,s)
   j+=1
