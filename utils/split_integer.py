#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2018/12/12 9:40

def split_integer(m,n):
    """
    将一个数均分
    :param m: 需要切分的数
    :param n: 份额
    :return:
    """
    assert n>0
    quotient = int(m/n)
    remainder = m%n
    if remainder >0:
        return [quotient]*(n-remainder)+[quotient+1]*remainder
    if remainder <0:
        return [quotient-1]*(-remainder)+[quotient]*(n+remainder)
    return [quotient]*n

def split_range(m,n):
    """
    将一个数分成相等的数据间隔
    :param m:
    :param n:
    """
    l = split_integer(m,n)
    x = 0
    y = -1
    i = 0
    t = []
    for interval in l:
        # x = i * interval
        x = y+1
        y = x + (interval - 1)
        if y > m:
            y = m
        i += 1

        t.append((x, y))
    return t
if __name__=="__main__":
    total = 283534
    partition = 9
    print(split_integer(10,3))
    print(split_range(10,3))
