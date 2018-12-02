#-*- coding: utf-8 -*-
# @Time    : 2018/10/4 0:09
# @Author  : Z
# @Email   : S
# @File    : demo2.py
import re
import requests as rq
from bs4 import BeautifulSoup
from  bs4.element import Tag
import time
import os
import sys
#获取一个页面的所有的视频播放页面的连接
def get_One_Page(page_url,rate):
    """

    :param page_url: 页面的url
    :return: hrefs  这一页每一个视频的播放地址
    """
    response=rq.get(url=page_url)
    html=response.text
    bs = BeautifulSoup(html,"html.parser")
    l = bs.find_all(attrs={"class":"video"})
    # print(l)
    hrefs=[]
    for i in range(len(l)):
        #获取  ' 54%'
        # rate_str = l[i].find_all(attrs={'class': 'video-rating text-success'})[0].get_text()
        r1=l[i].find_all(attrs={'class': 'video-rating text-success'})
        if len(r1)>0:
            rate_str = r1[0].get_text()

            rate_int = int(rate_str[1:-1])
            #设置分数条件
            if rate_int >=rate:
               print(rate_int)
               #获取href
               href = l[i].a.attrs['href']
               hrefs.append(href)
    return hrefs

# name='url'.split('/')[4]

def get_video(url):
    """

    :param url: 视频的播放页面   /37656/xxxx/
    :return:  视频的地址
    """
    print('开始获取视频的播放页面')
    start=time.time()
    response = rq.get(url=f"http://www.avtbg.com/{url}")
    html = response.text
    bs = BeautifulSoup(html, "html.parser")
    #获取视频的地址
    video_url = bs.video.source.attrs['src']

    end=time.time()
    print(f'获取视频播放页面结束 spent time:{end-start}s')
    return video_url

def download_video(video_url,name):
    print(f'start download {name}')

    start=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
    response = rq.get(url=video_url)
    with open(f"F:/BaiduNetdiskDownload/第12.13天（网络爬虫）/data/{name}.mp4","wb") as file:
            file.write(response.content)

    end=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
    print(f'stop download  {name}, spent time:{start}-{end}s')




if __name__=='__main__':
    # categorys=['babe','zhubo','ribenwuma']
    categorys=['ribenwuma']
    path= 'http://www.avtbg.com/'
    for category in categorys:
        first_page_url = f'{path}{category}/'
        if category=='babe':
            #共有多少页
            threshold= 343
            #只取分数大于等于rate的视频
            rate=90
            #从那一页开始
            i=264
        elif category=='zhubo':
            threshold = 63
            rate = 85
            i = 1
        else:
            threshold = 67
            rate = 85
            i = 28
        print(first_page_url)

        while i<=threshold:
           if i==1:
               next_page_url=first_page_url
           else:
               next_page_url=first_page_url+f'recent/{i}'
           print(next_page_url)

           hrefs = get_One_Page(page_url=next_page_url,rate=rate)
           for href in hrefs:
               try:
                   name = href.split('/')[2]
                   file=f"F:\\BaiduNetdiskDownload\\第12.13天（网络爬虫）\\data\\{name}.mp4"
                   if os.path.exists(file):
                         continue
                   video_url = get_video(href)
                   print(name)
                   download_video(video_url=video_url,name=name)
               except Exception  as e:
                   print(str(e),'下载失败')

           i+=1
