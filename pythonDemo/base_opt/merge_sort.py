#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/3/13 15:15
"""
归并排序
该算法是采用分治法（Divide and Conquer）的一个非常典型的应用。
将已有序的子序列合并，得到完全有序的序列。

基本过程：假设初始序列含有n个记录，则可以看成是n个有序的子序列，
每个子序列的长度为1，然后两两归并，得到n/2个长度为2或1的有序子序列，
再两两归并，最终得到一个长度为n的有序序列为止，这称为2路归并排序。
"""

def merge_sort(lst):
    """
    divide阶段
    :param lst:
    :return:
    """
    if len(lst)<=1:
        return lst
    middle=len(lst)//2
    left=merge_sort(lst[:middle])#不断递归，将原始序列拆分成n个小序列
    right=merge_sort(lst[middle:])
    return merge(left,right)

def merge(left,right):
    """
       conquer阶段
    :param left: 有序(正序)数组
    :param right: 有序(正序)数组
    :return: 整体有序(正序)的数组
    """
    i,j=0,0
    result=[]
    while i<len(left) and j<len(right):#比较两个子序列的元素，进行排序
        if left[i]>right[j]:
            result.append(right[j])
            j+=1
        else:
            result.append(left[i])
            i+=1
    result.extend(left[i:])
    result.extend(right[j:])
    return result


if __name__=="__main__":
    a=[1,3,5,7,9,2,4,6,8]
    print(merge_sort(a))