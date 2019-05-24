#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/5/23 15:48
from datetime import datetime as dt


from pysparkDemo.rules.retail import get_cust_info,get_acc,get_city_county,get_lng_lat,get_30_grade_change,card_passId,get_cust_index,\
                   get_consume_level_index,get_vfr_index,get_cpt_index
print(f"{str(dt.now())} -----retail")
# get_cust_info()
# get_acc()
# get_city_county()
# get_lng_lat()
# get_30_grade_change()
# card_passId()
# get_cust_index()
# get_consume_level_index()
# get_vfr_index()
# get_cpt_index()



from pysparkDemo.rules.area import get_city,get_trans_busiArea_index,get_gdp_index,get_ppl_areaCpt_index
print(f"{str(dt.now())} -----area")
# get_city()
# get_trans_busiArea_index()
# get_gdp_index()
# get_ppl_areaCpt_index()


from pysparkDemo.rules.block_data import get_block_item_top
print(f"{str(dt.now())} -----block_data")
# get_block_item_top()


from pysparkDemo.rules.macro_index import get_city,get_area_info,get_area_ppl
print(f"{str(dt.now())} -----macro_index")
# get_city()
# get_area_info()
# get_area_ppl()



from pysparkDemo.rules.yueyang_warning import get_qty_vfr_except,get_same_vfr_grade_excpet,get_around_order_except,get_around_item_except,\
                            get_grade_cons_except,get_high_cons_except,get_avg_cons_except

print(f"{str(dt.now())} -----yueyang_warning")

# get_qty_vfr_except()
# get_same_vfr_grade_excpet()
# get_around_order_except()
# get_around_item_except()
# get_grade_cons_except()
# get_high_cons_except()
# get_avg_cons_except()