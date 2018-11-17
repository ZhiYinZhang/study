#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/10/26 15:14
import requests
from bs4 import BeautifulSoup
import re
from urllib import parse
import time
import os
import uuid
"""
爬取百度图片  瀑布流形式的页面

在百度图片，使用关键字搜索，图片内容是动态加载的，通过向下滑动动态加载出来的
使用页面的url是没用图片信息的
当我们向下滑动，它是通过再访问一个url，返回一个json串，图片信息就在里面，然后插入到标签内。
f12打开开发者工具->Network->XHR->向下滑动页面  出现再次访问的url，这个url是动态变的，
通过观察发现，url的pn参数是变的，其他基本不变或没多大关系，而且pn是以30递增的,
所以我们直接访问这个url获取一个json串（点击Preview），在data里面获取图片数据的一个list，30个

"""

def get_image_url(url):
    """

    :param url: 下滑页面出现的的url
    :return: 图片url的list
    """
    print("开始获取图片地址")
    response = requests.get(url=url)
    result = response.json()
    image_url_list= []
    #遍历包含图片数据的list
    for item in result['data']:
        if item.get('thumbURL'):
          image_url = item['thumbURL']
          image_url_list.append(image_url)
    print("获取图片地址完成")
    return(image_url_list)

def download_image(urls,path):
    """
    下载图片并保存
    :param url: image url
           path:
    """
    print(f"开始下载图片，一共{len(urls)}张")
    i=0
    for url in urls:

       response = requests.get(url)
       if response.status_code==200:
           # time.sleep(1)
           i+=1
           u = uuid.uuid1()
           image_name = u.__str__().replace('-','')
           with open(file=f"{path}/{image_name}.jpg",mode='wb') as file:
               file.write(response.content)
               # print(response.content)
    print(f"下载完成")
    return i


if __name__=="__main__":
    keywords=["鼠","鹿","貂","猴","貘","树懒斑马","狗","狐","熊","象","豹子","麝牛","狮子小熊猫","疣猪","羚羊","驯鹿","考拉","犀牛","猞猁","穿山甲长颈鹿","熊猫","食蚁兽","猩猩","海牛","水獭","灵猫","海豚海象","鸭嘴兽","刺猬","北极狐","无尾熊","北极熊","袋鼠","犰狳河马","海豹","鲸鱼","鼬"]
    for keyword in keywords:
        print(f"-------------------------------------开始下载{keyword}---------------------------------------")
        #编码
        keyword_url=parse.quote(keyword)

        file_path = "E:\\javacode\\image\\"
        if not os.path.exists(path=file_path+keyword):
            os.makedirs(file_path+keyword)
        total=1
        #共下了多少张
        for i in range(30,600,30):
            print(f"pn {i}")
            url = f"https://image.baidu.com/search/acjson?tn=resultjson_com&ipn=rj&ct=201326592&is=&fp=result&queryWord={keyword_url}&cl=2&lm=-1&ie=utf-8&oe=utf-8&adpicid=&st=-1&z=&ic=0&word={keyword_url}&s=&se=&tab=&width=&height=&face=0&istype=2&qc=&nc=1&fr=&expermode=&pn={i}&rn=30&gsm=3c&1540608319195="
            image_urls =  get_image_url(url)

            num = download_image(urls=image_urls,path=file_path+keyword)
            total+=num
            print(f"--------------到目前为止下了{total}张---------------")
            if total>=150:
                break