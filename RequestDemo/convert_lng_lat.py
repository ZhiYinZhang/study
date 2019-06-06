#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/6/6 14:45
from datetime import datetime as dt
import requests as rq
import json

def convert_lng_lat(src_path,result_path,key):
    with open(src_path,"r",encoding="utf-8") as reader:
         records=reader.readlines()

    with open(result_path,"w",encoding="utf-8") as writer:
        colNames=records[0].split("\n")[0]+",lng,lat\n"
        writer.write(colNames)

        results=[]
        for i in records[1:]:
            row=i.split("\n")[0].split(",")
            cust_id=row[0]
            city=row[1]
            busi_addr=row[2]

            url = f"https://restapi.amap.com/v3/geocode/geo?key={key}&address={busi_addr}&city={city}&output=json"
            try:
                res = rq.get(url=url)

                location = json.loads(res.text)["geocodes"][0]["location"]
                lng_lat = location.split(",")
                lng = lng_lat[0]
                lat = lng_lat[1]
            except:
                print(json.loads(res.text))
                lng=''
                lat=''

            result=f"{cust_id},{city},{busi_addr},{lng},{lat}\n"

            results.append(result)

        writer.writelines(results)

if __name__=="__main__":
    #高德api 需要购买key
    key = "e5c7aff5f51f96e8ef3d90aea5148cc2"
    # cust_id city busi_addr
    src_path = "E:/chromeDownload/cust_addr.csv"
    # cust_id city busi_addr lng lat
    result_path="e:/test/cust_lng_lat.csv"

    print(str(dt.now()))
    convert_lng_lat(src_path,result_path,key)
    print(str(dt.now()))