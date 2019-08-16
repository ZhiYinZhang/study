# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 17:23:53 2019

@author: user
"""

import pandas as pd
import numpy as np
from itertools import product
import itertools
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import *


############### preprocess ###############
def get_demand(x):
    if x['qty_need'] <= x['qty_rsn']:
        return x['qty_ord']
    else:
        return x['qty_ord']+1


def floor_to_week(x):
    return x+datetime.timedelta(days=1)-pd.offsets.Week(weekday=0)


def prepocessing(df):
    df[['qty_need','qty_ord','qty_rsn','price','amt']] = df[['qty_need','qty_ord','qty_rsn','price','amt']].astype('float')
    df['need_ord_diff'] = df['qty_need'] - df['qty_ord']
    df['rsn_ord_diff'] = df['qty_rsn'] - df['qty_ord']
    df['is_purchase'] = (df['qty_ord']>0).astype('int')
    df['ord_less_rsn'] = df['rsn_ord_diff']>0
    df['ord_less_need'] = df['need_ord_diff']>0
    df['born_date'] = pd.to_datetime(df['born_date'])
    df['born_date_week'] = floor_to_week(df['born_date'])
    return df


############ feature engineering ################
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 17 10:05:55 2019

@author: user
"""

import pandas as pd
import numpy as np
from itertools import product
import itertools
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import *


############### preprocess ###############
def get_demand(x):
    if x['qty_need'] <= x['qty_rsn']:
        return x['qty_ord']
    else:
        return x['qty_ord']+1


def floor_to_week(x):
    return x+datetime.timedelta(days=1)-pd.offsets.Week(weekday=0)


def prepocessing(df):
    df[['qty_need','qty_ord','qty_rsn','price','amt']] = df[['qty_need','qty_ord','qty_rsn','price','amt']].astype('float')
    df['need_ord_diff'] = df['qty_need'] - df['qty_ord']
    df['rsn_ord_diff'] = df['qty_rsn'] - df['qty_ord']
    df['is_purchase'] = (df['qty_ord']>0).astype('int')
    df['ord_less_rsn'] = df['rsn_ord_diff']>0
    df['ord_less_need'] = df['need_ord_diff']>0
    df['born_date'] = pd.to_datetime(df['born_date'])
    df['born_date_week'] = floor_to_week(df['born_date'])
    return df


############ feature engineering ################
def create_basic_feat(df):
    
    '''
    订单的qty_need, qty_ord, qty_rsn, need_ord_diff, rsn_ord_diff按品规和日期
    做groupby，统计sum, max, min, median, mean, std, skew, kurt, 
    用is_purchase, ord_less_rsn, ord_less_need按品规和日期做groupby，统计sum和mean，
    所有的统计值作为该品规该周的基本供需特征
    '''
    
    train_df = df.groupby(['item_id','sale_center_id','born_date_week'],as_index=False).agg({
    'qty_need':['sum','max','min','median','mean','std','skew',pd.Series.kurtosis],
    'qty_ord':['sum','max','min','median','mean','std','skew',pd.Series.kurtosis],
    'qty_rsn':['sum','max','min','median','mean','std','skew',pd.Series.kurtosis],
    'need_ord_diff':['sum','max','min','median','mean','std','skew',pd.Series.kurtosis],
    'rsn_ord_diff':['sum','max','min','median','mean','std','skew',pd.Series.kurtosis]
    })
    cols = ['item_id','sale_center_id','born_date_week']+[x+'_'+y for x,y in list(product(['qty_need','qty_ord','qty_rsn','need_ord_diff','rsn_ord_diff'],
                                 ['sum','max','min','median','mean','std','skew','kurt']))]
    train_df.columns = cols
    
    feat_df = df.groupby(['item_id','sale_center_id','born_date_week'],as_index=False).agg({
    'is_purchase':['sum','mean'],
    'ord_less_rsn':['sum','mean'],
    'ord_less_need':['sum','mean'],
    })
    feat_df.columns = ['item_id','sale_center_id','born_date_week']+[x+'_'+y for x,y in list(product(['is_purchase','ord_less_rsn','ord_less_need'],
                                     ['sum','mean']))]
    train_df = train_df.merge(feat_df, on=['item_id','sale_center_id','born_date_week'], how='left')
    
    # 当周有记录、有下单、没下单、退货的零售户数量
    pur_cust = df.groupby(['item_id','sale_center_id','born_date_week','cust_id'],as_index=False).agg({'qty_ord':['sum']})
    pur_cust.columns = ['item_id','sale_center_id','born_date_week','cust_id'] + [x+'_'+y for x,y in list(product(['qty_ord'],['sum']))]
    pur_cust['cust_purchase'] = pur_cust['qty_ord_sum']>0
    pur_cust['cust_return'] = pur_cust['qty_ord_sum']<0
    pur_cust_count = pur_cust.groupby(['item_id','sale_center_id','born_date_week'],as_index=False).agg({'cust_purchase':['sum','count'],'cust_return':['sum']})
    pur_cust_count.columns = ['item_id','sale_center_id','born_date_week']+[x+'_'+y for x,y in list(product(['cust_purchase'],['sum','count']))]+['cust_return_sum']
    pur_cust_count['cust_not_purchase'] = pur_cust_count['cust_purchase_count'] - pur_cust_count['cust_purchase_sum']
    
    # 当周有记录、有下单、没下单、退货的零售户占比
    pur_cust_count['ratio_pur_cust'] = (pur_cust_count['cust_purchase_sum']+1)/(pur_cust_count['cust_purchase_count']+1)
    pur_cust_count['ratio_not_pur_cust'] = (pur_cust_count['cust_not_purchase']+1)/(pur_cust_count['cust_purchase_count']+1)
    pur_cust_count['ratio_not_pur_cust'] = (pur_cust_count['cust_not_purchase']+1)/(pur_cust_count['cust_purchase_count']+1)
    
    # 拼接主dataframe
    train_df = train_df.merge(pur_cust_count, on=['item_id','sale_center_id','born_date_week'], how='left')
    
    
    # 零售户平均购买间隔与当周距离上次记录间隔的差
    interval_df = df[['item_id','sale_center_id','cust_id','born_date_week','qty_rsn','qty_ord']].sort_values(['item_id','sale_center_id','cust_id','born_date_week'])
    interval_df = interval_df.dropna()
    
    # 零售户平均购买间隔
    pur_int_df = interval_df[interval_df['qty_ord']>0]
    pur_int_df['last_pur'] = pur_int_df.groupby(['item_id','sale_center_id','cust_id'],as_index=False)['born_date_week'].shift(1)
    pur_int_df['pur_interval'] = (pur_int_df['born_date_week']-pur_int_df['last_pur']).dt.days
    pur_int_df.dropna(subset=['pur_interval'],inplace=True)
    pur_int_df = pur_int_df.groupby(['item_id','sale_center_id','cust_id'],as_index=False).agg({'pur_interval':'mean'})
    interval_df = interval_df.merge(pur_int_df, on=['item_id','sale_center_id','cust_id'], how='left')
    
    # 零售户当周距离上次记录间隔
    interval_df['last_record'] = interval_df.groupby(['item_id','sale_center_id','cust_id'],as_index=False)['born_date_week'].shift(1)
    interval_df['record_interval'] = (interval_df['born_date_week']-interval_df['last_record']).dt.days
    
    # 平均购买间隔-当周记录间隔
    interval_df['diff_pur_interval_record_interval'] = interval_df['pur_interval'] - interval_df['record_interval']
    
    # 聚合为供需特征
    interval_df = interval_df=interval_df.groupby(['item_id','sale_center_id','born_date_week'],as_index=False).agg({
        'pur_interval':['sum','max','min','median','mean','std','skew',pd.Series.kurtosis],
        'record_interval':['sum','max','min','median','mean','std','skew',pd.Series.kurtosis],
        'diff_pur_interval_record_interval':['sum','max','min','median','mean','std','skew',pd.Series.kurtosis]
    })
    cols = ['item_id','sale_center_id','born_date_week']+[x+'_'+y for x,y in list(product(['pur_interval','record_interval','diff_pur_interval_record_interval'],
                                     ['sum','max','min','median','mean','std','skew','kurt']))]
    interval_df.columns = cols
    
    # 拼接主dataframe
    train_df = train_df.merge(interval_df, on=['item_id','sale_center_id','born_date_week'], how='left')
    
    return train_df


def pad_ts(df):
    start = df['born_date_week'].unique().min()
    end = df['born_date_week'].unique().max()
    time = pd.date_range(start,end,freq='7D')
    item = list(df['item_id'].unique())
    sale_center = list(df['sale_center_id'].unique())
    all_df=pd.DataFrame(list(map(list,itertools.product(item,sale_center,time))), columns=['item_id','sale_center_id','born_date_week'])
    pad_df = all_df.merge(df, on=['item_id','sale_center_id','born_date_week'],how='left')
    pad_df = pad_df.sort_values(['item_id','sale_center_id','born_date_week'])
    return pad_df

def create_holiday_feat(holiday_df, df, tm_col='born_date_week'):
    # 节假日
    pad_train_df = pd.merge_asof(df.sort_values(tm_col), holiday_df, 
              left_on=tm_col, right_on='time', 
              tolerance=pd.Timedelta('7d'), direction='backward')
    pad_train_df['holiday_type'] = pad_train_df['holiday_type'].fillna(0)
    pad_train_df = pad_train_df.sort_values(['item_id',tm_col])
    del pad_train_df['time']
    return pad_train_df

def create_date_feat(df,tm_col='born_date_week'):
    # 月份，一年第几周，一月第几周
    pad_train_df = df.copy()
    pad_train_df['month'] = pad_train_df[tm_col].dt.month
    pad_train_df['weekofyear'] = pad_train_df[tm_col].dt.weekofyear
    pad_train_df['weekinmonth'] = pad_train_df[tm_col].dt.day
    pad_train_df['weekinmonth'] = pad_train_df['weekinmonth']//7
    return pad_train_df
    
def max_minus_min(x):
    return np.max(x)-np.min(x)

def mean_minus_min(x):
    return np.mean(x)-np.min(x)

def mean_minus_max(x):
    return np.mean(x)-np.max(x)

def get_num_feat(feat,X_df,win):
    X_df[feat+'_rolling_5_mean'] = X_df.groupby(['item_id','sale_center_id'],as_index=True)[feat].rolling(window=win,min_periods=1).mean().values
    X_df[feat+'_rolling_5_min'] = X_df.groupby(['item_id','sale_center_id'],as_index=True)[feat].rolling(window=win,min_periods=1).min().values
    X_df[feat+'_rolling_5_max'] = X_df.groupby(['item_id','sale_center_id'],as_index=True)[feat].rolling(window=win,min_periods=1).max().values
    X_df[feat+'_rolling_5_max_minus_min'] = X_df.groupby(['item_id','sale_center_id'],as_index=True)[feat].rolling(window=win,min_periods=1).\
                                            apply(max_minus_min).values
    X_df[feat+'_rolling_5_mean_minus_min'] = X_df.groupby(['item_id','sale_center_id'],as_index=True)[feat].rolling(window=win,min_periods=1).\
                                            apply(mean_minus_min).values
    X_df[feat+'_rolling_5_mean_minus_max'] = X_df.groupby(['item_id','sale_center_id'],as_index=True)[feat].rolling(window=win,min_periods=1).\
                                            apply(mean_minus_max).values 
    return X_df

def create_rolling_feat(df,feats,win,n=None):
    
    '''
    对供需特征做延后n周处理（n=预测未来第n周的n），
    然后对供需特征做5周rolling的mean,max,min,max-min,mean-min,mean-max得到rolling_5特征
    '''
    
    X_df = df.copy()
    if n != None:
        X_df['born_date_week'] = X_df['born_date_week'] + pd.Timedelta(str(7*n)+'d')
    
    for feat in tqdm(feats):
        X_df = get_num_feat(feat,X_df,win)
                                                
    return X_df

def create_expected_historical_feat(holiday_df,X_df,tm_col='born_date_week'):
    
    # 未来一周为节假日
    fut = 1
    fut_holiday_type = holiday_df.copy()
    fut_holiday_type['time'] = fut_holiday_type['time'] - pd.Timedelta(str(7*fut)+'d')
    fut_holiday_type.rename(columns={'holiday_type':'fut'+str(fut)+'_holiday_type'},inplace=True)
    X_df = pd.merge_asof(X_df.sort_values(tm_col), fut_holiday_type, left_on=tm_col,right_on='time',
                        tolerance=pd.Timedelta('7d'), direction='backward')
    X_df = X_df.sort_values(['item_id',tm_col])
    del X_df['time']
    X_df['fut'+str(fut)+'_holiday_type'].fillna(0,inplace=True)
    del fut_holiday_type
    
    # 过去一周为节假日
    his = 1
    his_holiday_type = holiday_df.copy()
    his_holiday_type['time'] = his_holiday_type['time'] + pd.Timedelta(str(7*his)+'d')
    his_holiday_type.rename(columns={'holiday_type':'his'+str(his)+'_holiday_type'},inplace=True)
    X_df = pd.merge_asof(X_df.sort_values(tm_col), his_holiday_type, left_on=tm_col,right_on='time',
                        tolerance=pd.Timedelta('7d'), direction='backward')
    X_df = X_df.sort_values(['item_id',tm_col])
    del X_df['time']
    X_df['his'+str(his)+'_holiday_type'].fillna(0,inplace=True)
    del his_holiday_type
    
    return X_df

def create_y(df,feat_cols):
    
    '''
    创建y值列
    '''
    
    y_df = df[['item_id','sale_center_id','born_date_week','qty_ord_sum']+feat_cols].fillna(0)
    y_df.rename(columns={'qty_ord_sum':'label'},inplace=True)
    # print('y_df columns:',y_df.columns)
    y_df['log_label'] = np.log(y_df['label']+100)
    y_df['purchase_label'] = y_df['label']>0
    return y_df



    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
        