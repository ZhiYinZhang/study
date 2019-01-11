#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/9 10:40
import re
import requests as rq
from bs4 import BeautifulSoup
from bs4.element import ResultSet,Tag
def get_searchUrl(searchString):
    chars = {':': "%3A", '?': '%3F', '’': '%E2%80%99', '!': '%21', '/': "%2F", "—": '%E2%80%94', '+': '%2B'}
    chan = {'–': '%E2%80%93', }
    url1 = "https://www.slideshare.net/search/slideshow?searchfrom=header&q="
    url2 = "&ud=any&ft=all&lang=**&sort="

    s_list = searchString.split(" ")
    result = ""
    for i in s_list:
        for c in chars.keys():
            i = i.replace(c, chars[c])
        if i == '–':
            i = i.replace('–', '%E2%80%93')

        result += (i + "+")
    result = result[:-1]

    url = "%s%s%s"%(url1,result,url2)
    return url



def get_download_page_url(search_page_url):
    time = 0
    download_page_url= ''
    while time <= 3:
        rp = rq.get(url=search_page_url)
        if rp.status_code != 200:
            time += 1
            print(f"请求{url}失败{time}次")
            continue

        html = rp.text
        bs = BeautifulSoup(html, 'html.parser')
        r1: ResultSet = bs.find_all(attrs={"class": 'thumbnail-content'})

        for i in r1:
            ele: Tag = i
            author = ele.find(attrs={"class": "author"}).text.strip("\n").strip(" ")[:-1]
            if author == "Databricks":
                # 下载页面的url
                download_page_url = "https://www.slideshare.net" + ele.a.get('href')
        break
    return download_page_url
def get_downloadUrl(download_page_url):
    rp = rq.get(download_page_url)
    html = rp.text

    bs4 = BeautifulSoup(html,"html.parser")
    print(bs4.find_all(attrs={"data-action":"download"}))



if __name__=="__main__":
    s = "Streaming Trend Discovery: Real-Time Discovery in a Sea of Events – continues"
    #搜索出来的页面的url
    # search_page_url = get_searchUrl(s)
    #
    # download_page_url = get_download_page_url(search_page_url)
    #
    # get_downloadUrl(download_page_url)

    rp = rq.get(url="https://www.slideshare.net/savedfiles?s_title=streaming-trend-discovery-realtime-discovery-in-a-sea-of-events-with-scott-haines&user_login=databricks")
    print(rp.text)