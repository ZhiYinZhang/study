#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2018/12/12 9:40

def split_integer(total,partition):
    """
    将一个数均分
    :param total: 需要切分的数
    :param partition: 分区
    :return:  每个分区的间隔
    """
    assert partition>0
    quotient = int(total/partition)
    remainder = total%partition
    if remainder >0:
        return [quotient]*(partition-remainder)+[quotient+1]*remainder
    if remainder <0:
        return [quotient-1]*(-remainder)+[quotient]*(partition+remainder)
    return [quotient]*partition

def split_range(total,partition):
    """
    将一个数分成相等的数据间隔
    :param total:
    :param partition:
    """
    ranges = []
    if total>1*1024:
        intervals = split_integer(total,partition)
        x = 0
        y = -1
        i = 0

        for interval in intervals:
            x = y+1
            y = x + (interval - 1)

            i += 1
            if i == len(intervals):
                if y != total:
                    y = total

            ranges.append((x, y))
    else:
       ranges.append((0,total))
    return ranges

def split_range_zip(total_size,partition):
    """
    使用zip
    :return:
    """
    s = 1*1021*1024
    l1 = []
    if total_size>s:
        total = range(total_size + 1)
        #每个partition的间隔
        p = len(total) // partition

        #将total分成total_size/p份
        z = zip(*[iter(total)] * p)
        l = list(z)

        for i in l:
            l1.append((i[0], i[-1]))
            #最后一个
            if i == l[-1]:
                #如果total_size与最后一个相差大于s/2（无意义），多增加一个间隔
                if total_size-i[-1]>s/2:
                    l1.append((i[-1] + 1, total_size))
                # 如果total_size与最后一个相差小于s/2（无意义），替换最后一个间隔
                elif total_size-i[-1]<=s/2:
                    l1.pop(-1)
                    l1.append((i[0], total_size))
    else:
       l1.append((0,total_size))
    return l1
if __name__=="__main__":
    import time
    start  = time.time()
    total_size = 3
    partition = 3
    # l = split_range_zip(total_size,partition)   #4.438253879547119

    inters = split_integer(total_size,partition)
    l = split_range(total_size,partition)
    print(inters)
    print(l)

    # [3333, 3333, 3334]
    # [(0, 3332), (3333, 6665), (6666, 10000)]



    stop = time.time()
    print(stop-start)