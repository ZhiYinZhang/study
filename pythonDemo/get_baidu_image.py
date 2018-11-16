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
       if response.status_code()==200:
           u = uuid.uuid1()
           name = u.__str__().replace('-','')
           response = requests.get(url)
           i+=1
           with open(file=f"{path}/{name}.jpg",mode='wb') as file:
               file.write(response.content)
    print(f"下载完成")


if __name__=="__main__":

    keyword="美女"
    #编码
    keyword_url=parse.quote(keyword)

    file_path = "E:\\test\\image\\"
    if not os.path.exists(path=file_path+keyword):
        os.makedirs(file_path+keyword)

    for i in range(30,100,30):
        url = f"http://image.baidu.com/search/acjson?tn=resultjson_com&ipn=rj&ct=201326592&is=&fp=result&queryWord={keyword_url}&cl=2&lm=-1&ie=utf-8&oe=utf-8&adpicid=&st=&z=&ic=&word={keyword_url}&s=&se=&tab=&width=&height=&face=&istype=&qc=&nc=&fr=&expermode=&pn={i}&rn=30&gsm=78&1540543731419="
        image_urls =  get_image_url(url)

        download_image(urls=image_urls,path=file_path+keyword)