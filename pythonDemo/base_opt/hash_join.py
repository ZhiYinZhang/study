#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/3/18 11:01


def hash_join(build_table,probe_table):
    """
    基本的hash join
    shuffle hash join 和broadcast hash join都属于hash join
    只是在hash join之前是shuffle还是broadcast
    broadcast适合小表和大表:先将小表广播到每个executor，然后在每个executor进行hash join
    shuffle：是将两个表相同的key分到同一个executor上，然后进行hash join


    这里只列出join key
       build_table 小表
       probe_table 大表
       hash_table  哈希表
       result      结果表

       build_table可以全部加载到内存，就不分区
       分10个分区
       1.构建hash_table：将build_table遍历，根据join key进行hash，hash到对应的bucket，生成hash table中一条记录
       2.探测：遍历probe_table，使用相同的hash函数映射hash_table中的记录(判断hash code是否相等)，
       映射成功再检查join条件,如果符合条件就将两者join在一起

    """
    hash_table = {}
    result = []
    # 构建hash_table
    for i in build_table:
        id = hash(i) % 10
        if not hash_table.get(id):
            hash_table[id] = [i, ]
        else:
            hash_table[id].append(i)

    # {1: [1, 1], 3: [3, 3, 3], 4: [4], 6: [6], 8: [8, 8], 0: [10]}
    # print(hash_table)

    for i in probe_table:
        id = hash(i) % 10
        # 映射  如果probe_table中的值的hash在hash_table的key中
        if id in hash_table.keys():
            for h_value in hash_table[id]:
                # join 条件
                if h_value == i:
                    result.append((h_value, i))

    # [(1, 1), (1, 1), (3, 3), (3, 3), (3, 3), (4, 4), (6, 6), (6, 6), (10, 10)]
    print(result)
if __name__=="__main__":
    build_table = [1, 1, 3, 3, 3, 4, 6, 8, 8, 10]
    probe_table = [0, 0, 1, 2, 2, 3, 4, 5, 6, 6, 9, 9, 9, 10]

    i=0
    j=0
    # while i<len(build_table) and j<len(probe_table):


