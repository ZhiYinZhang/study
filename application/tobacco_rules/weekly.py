#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/6/6 17:32
from datetime import datetime as dt


from rules.generate_data import generate_around_vfr,generate_all_cust_cons
print(f"{str(dt.now())}------------generate data")
generate_around_vfr(2)#30min->6min
# generate_all_cust_cons()


from rules.retail import get_cust_info,get_acc,get_city_county,get_lng_lat,get_30_grade_change,get_cust_index,\
                   get_consume_level_index,get_vfr_index,get_cpt_index
from rules.retail import get_order_stats_info_weekly,get_ratio_weekly


print(f"{str(dt.now())} ----------retail")
get_cust_info()#6
get_acc()#13s
get_city_county()#7s
get_lng_lat()#5s
get_30_grade_change()#2min
get_cust_index()#23min->3min
get_consume_level_index()#20min->4min
get_vfr_index()#12s
get_cpt_index()#1min


get_order_stats_info_weekly()#1min
get_ratio_weekly()#4min



from rules.area import get_city,get_ppl_areaCpt_index
from rules.area import get_order_stats_info_daily,get_order_stats_info_weekly,get_ratio_weekly
print(f"{str(dt.now())} ----------area")
get_city()#3s
get_ppl_areaCpt_index()#4s

get_order_stats_info_daily()#30s
get_order_stats_info_weekly()#1min
get_ratio_weekly()#4min



from rules.block_data import get_block_item_top
print(f"{str(dt.now())} ----------block_data")
get_block_item_top()#36min->23min


from rules.generate_data import generate_cust_cluster
print(f"{str(dt.now())}----------cluster")
generate_cust_cluster()#70min

from rules.yueyang_warning import get_warning
get_warning()#10min

#已移除
# from rules.yueyang_warning import get_qty_vfr_except,get_same_vfr_grade_excpet,get_around_order_except,get_around_item_except,\
#                             get_grade_cons_except,get_high_cons_except,get_avg_cons_except
#
# print(f"{str(dt.now())} ----------yueyang_warning")
# from rules.config import warning_code_table
# delete_all(warning_code_table,"CUST_ID")
# get_qty_vfr_except()
# get_same_vfr_grade_excpet()
# get_around_order_except()
# get_around_item_except()
# get_grade_cons_except()
# get_high_cons_except()
# get_avg_cons_except()


print(f"{str(dt.now())} end")





