#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/11 13:12
import math

def lng_l_r(lng,lat):
      lng_r=lng+1/(111.11*math.cos(lat))
      lng_l=lng-1/(111.11*math.cos(lat))
      return (lng_l,lng_r)
def lat_d_u(lat):
      lat_u=lat+1/111.11
      lat_d=lat-1/111.11
      return (lat_d,lat_u)

if __name__=="__main__":
     # for i in range(361):
     #       print(i)
     #       lng=lng_l_r(i,10)
     #
     #       if(lng[0]>lng[1]):
     #             print(lng)


     for i in range(0,361,10):
           print(i,math.cos(i))


