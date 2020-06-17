#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/6/6 14:45
from datetime import datetime as dt
import requests as rq
import json
import math
import traceback as tb
def convert_lng_lat(src_path,result_path,index,key="e5c7aff5f51f96e8ef3d90aea5148cc2",batch=10):
    """

    :param src_path:文件头 cust_id,city,busi_addr
    :param result_path:
    :param key:
    :param index:{"city":1,"addr":2} 城市字段是第几列 地址字段是第几列 从0开始
    :param batch:
    """
    with open(src_path,"r",encoding="utf-8") as reader:
         records=reader.readlines()

    with open(result_path,"w",encoding="utf-8") as writer:
        #获取csv 列名 并添加 lng，lat
        colNames=records[0].split("\n")[0]+",lng,lat\n"
        writer.write(colNames)

        succ=0
        fail=0

        results=[]
        for i in records[1:]:
            #获取这一行的数据 list
            row=i.split("\n")[0].split(",")

            #获取城市 地址
            city=row[index["city"]]
            busi_addr=row[index["addr"]]

            url = f"https://restapi.amap.com/v3/geocode/geo?key={key}&address={busi_addr}&city={city}&output=json"
            try:
                res = rq.get(url=url)

                location = json.loads(res.text)["geocodes"][0]["location"]
                #高德经纬度
                gd_lng_lat = location.split(",")
                # lng = gd_lng_lat[0]
                # lat = gd_lng_lat[1]
                print(f"高德:{gd_lng_lat}")
                #高德-》百度
                bd_lng_lat=gcj02_to_bd09(float(gd_lng_lat[0]),float(gd_lng_lat[1]))
                lng = bd_lng_lat[0]
                lat = bd_lng_lat[1]

                succ+=1
            except:
                tb.print_exc()
                print(json.loads(res.text))
                lng=''
                lat=''
                fail+=1

            result=f"{','.join(row)},{lng},{lat}\n"
            print(result)
            results.append(result)

            if len(results)==batch:
                writer.writelines(results)
                writer.flush()
                results.clear()

            print(f"success:{succ},failure:{fail}")
        #将剩余的写入
        writer.writelines(results)

def gcj02_to_bd09(lng, lat):
     """
     火星坐标系(GCJ-02)转百度坐标系(BD-09)
     谷歌、高德——>百度
     :param lng:火星坐标经度
     :param lat:火星坐标纬度
     :return:
     """
     x_pi=math.pi*3000.0/180.0
     z = math.sqrt(lng * lng + lat * lat) + 0.00002 * math.sin(lat * x_pi)
     theta = math.atan2(lat, lng) + 0.000003 * math.cos(lng * x_pi)
     bd_lng = z * math.cos(theta) + 0.0065
     bd_lat = z * math.sin(theta) + 0.006
     return [bd_lng, bd_lat]

if __name__=="__main__":
    #高德api 需要购买key
    key = "e5c7aff5f51f96e8ef3d90aea5148cc2"

    # cust_id,city,busi_addr,com_id
    src_path = "E:/test/cust_lng_lat/20200617/cust_addr.csv"
    # cust_id,city,busi_addr,com_id,lng,lat
    result_path="e:/test/cust_lng_lat/20200617/cust_lng_lat.csv"

    print(str(dt.now()))
    convert_lng_lat(src_path,result_path,index={"city":0,"addr":3},key=key)
    print(str(dt.now()))
