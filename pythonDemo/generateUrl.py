#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/7 15:51
import random
import os
def generateUrl():
    word = "qwertyuiopasdfghjklzxcvbnm"
    url = "sparkdocs/latest/structured-streaming-programming-guide.html"
    limit = 100

    fileUrl1 = "e://test//checkpoint//URL1.txt"
    fileUrl2 = "e://test//checkpoint//URL2.txt"

    url1s = []
    url2s = []
    x=0
    while True:
        for y in range(1000):
            l1 = random.sample(word,4)
            d1 = ''.join(l1)

            l2 = random.sample(word, 4)
            d2 = ''.join(l2)

            url1=url+d1+"\n"
            url2=url+d2+"\n"

            url1s.append(url1)
            url2s.append(url2)
        print(x)
        x+=1
        with open(fileUrl1,"a") as writer1:
            writer1.writelines(url1s)
            url1s.clear()

        with open(fileUrl2,"a") as writer2:
            writer2.writelines(url2s)
            url2s.clear()

        if os.path.getsize(fileUrl1)>1024*1024*1024 and os.path.getsize(fileUrl2)>1024*1024*1024:
            break

def generateWord():
    word = "qwertyuiopasdfghjklzxcvbnm"

    w = "0123456789"
    file = "e://test//checkpoint//word.txt"
    ws = []
    while True:
        for y in range(1000):
            l1 = random.sample(word, 6)
            d1 = ''.join(l1)
            w1 = w + d1 + "\n"
            ws.append(w1)


        with open(file, "a") as writer1:
            writer1.writelines(ws)

        if os.path.getsize(file)>1024*1024*1024:
            break

if __name__=="__main__":
    # generateUrl()
    generateWord()