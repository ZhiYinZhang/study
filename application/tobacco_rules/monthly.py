#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/8/5 14:22

from rules.retail import get_order_stats_info_monthly,get_order_yoy_info,get_ratio_monthly

get_order_stats_info_monthly()#45s
get_order_yoy_info()#1min
get_ratio_monthly()#15min



from rules.area import get_trans_busiArea_index,get_gdp_index
from rules.area import get_order_stats_info_monthly,get_order_yoy_info,get_ratio_monthly

get_trans_busiArea_index()#11s
get_gdp_index()#1s

get_order_stats_info_monthly()#2min
get_order_yoy_info()#1min
get_ratio_monthly()#12min


from rules.macro_index import get_city,get_area_info,get_area_ppl
print(f"{str(dt.now())} ----------macro_index")
get_city()#2s
get_area_info()#3s
get_area_ppl()#5s