#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/11/18 14:29
"""
breadth first search ，BFS 广度优先搜索,广度优先遍历

a->b->d->e->g->h
 ↘     ↘ ↓   ↗
  -->c-->f-->

↖↑↗←→↙↓↘
"""
from collections import deque
def data_init():
    graph={}
    graph["a"]=["b","c"]
    graph["b"]=["d"]
    graph["c"]=["f"]
    graph["d"]=["e","f"]
    graph["e"]=["f","g"]
    graph["f"]=["h"]
    graph["g"]=["h"]
    return graph

def search(start,end,graph):
    # 记录等待搜索的节点
    search_deque = deque()

    search_deque += graph[start]

    # 记录搜索过的节点
    searched = [start]

    while search_deque:  # 等待搜索的节点的队列不为空
        node = search_deque.popleft()  # 从左边移除一个节点出来
        if node not in searched:  # 判断节点是否已搜索过
            """
            不同的场景，就是这个条件不同，说明使用BFS已经搜索到符合条件的节点
            如人际关系图，搜索到是老师的退出
            """
            if node == end:#判断当前节点是否是终点
                searched.append(end)
                return searched
            else:#不符合条件，将这个节点的下面的节点添加到待搜索的队列里面
                search_deque += graph[node]
                searched.append(node)
def print_bfs_path(start,end,lst,graph):
    path=[end]
    pointer=end
    while pointer!=start:
        for node in lst:
            if pointer in graph[node]:
                pointer=node
                path.append(node)
                break
    print(path[::-1])
if __name__=="__main__":
    graph = data_init()

    start = "b"
    end = "f"

    #BFS遍历搜索
    searched=search(start,end,graph)

    print(searched)

    print_bfs_path(start,end,searched,graph)