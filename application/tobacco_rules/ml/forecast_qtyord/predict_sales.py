# -*- coding: utf-8 -*-
"""
Created on Mon Jun 17 11:06:27 2019

@author: user
"""

from ml.forecast_qtyord.utils import *
from ml.forecast_qtyord.train_model import *
import numpy as np
import datetime
import gc
import pandas as pd

#@pysnooper.snoop()
def predict(df,n):
    
    '''
    input args:
        data_path: path of dataset
        n: the number of the future week to predict
        
    output:
        sub_df: predicted result, rows are item_id and columns are dates
    '''
    
    # df = pd.read_parquet(data_path) # read data, this line of code need to be modified
    
    ########## preprocess ###########
    df = prepocessing(df)
    
    print(df.head())
    ########## feature engineer ###########
    
    '''
    已完成特征：
    
    1. 订单的qty_need, qty_ord, qty_rsn, need_ord_diff, rsn_ord_diff按品规和日期做groupby，统计sum, max, min, median, mean, std, skew, kurt, 用is_purchase, ord_less_rsn, ord_less_need按品规和日期做groupby，统计sum和mean，所有的统计值作为该品规该周供需特征
    
    2. 对供需特征做延后n周处理（n=预测未来第n周的n），然后对供需特征做5周rolling的mean,max,min,max-min,mean-min,mean-max得到rolling_5特征
    
    3. 基础供需特征: 统计当周有记录、有下单、没下单、退货的零售户数量及占比，零售户对品规平均购买间隔与当周距离上次购买间隔的差的mean, max, min, sum, median, std, skew, kurt
    
    4. 节假日
    '''
    
    holiday_df = pd.DataFrame([
                    ('2018-04-29',1),
                    ('2018-06-16',1),
                    ('2018-09-22',1),
                    ('2018-10-01',1),
                    ('2018-12-30',2),
                    ('2019-02-04',3),
                    ('2019-02-10',3),
                    ('2019-04-05',1),
                    ('2019-05-01',1),
                    ('2019-06-07',1),
                    ('2019-09-13',1),
                    ('2019-10-01',1),
                    ],
            columns=['time','holiday_type'])
    holiday_df['time'] = pd.to_datetime(holiday_df['time'])
    
    df = create_basic_feat(df)
    pad_df = pad_ts(df)
    train_pad_df = create_holiday_feat(holiday_df,pad_df)
    train_pad_df = create_date_feat(train_pad_df)
    train_pad_df = create_expected_historical_feat(holiday_df,train_pad_df)
    # print('-'*50)
    # for c in train_pad_df.columns:
    #     print(c)
    # print('-'*50)
    not_ts_feat = ['month','weekofyear','weekinmonth']+[x for x in train_pad_df.columns if x.endswith('holiday_type')]
    # print(not_ts_feat)
    ts_num_feat_cols = [x for x in pad_df.columns if x not in ['item_id','sale_center_id','born_date_week','time']+not_ts_feat]
    y_df = create_y(train_pad_df,not_ts_feat)
    # print(y_df.columns)
    train_pad_df = create_rolling_feat(train_pad_df, ts_num_feat_cols, win=5, n=n)
    train_df = y_df.merge(train_pad_df[[x for x in train_pad_df.columns if x not in not_ts_feat]], on=['item_id','sale_center_id','born_date_week'], how='left')

    print(train_df.head())
    
    # map item_id,sale_center_id to int
    inv_item_mapping = dict(zip(range(len(train_df.item_id.unique())), 
                            train_df.item_id.unique()))
    item_mapping = dict(zip(train_df.item_id.unique(),
                            range(len(train_df.item_id.unique()))))
    sale_center_mapping = dict(zip(train_df.sale_center_id.unique(),
                        range(len(train_df.sale_center_id.unique()))))
    inv_sale_center_mapping = dict(zip(range(len(train_df.sale_center_id.unique())), 
                                train_df.sale_center_id.unique()))
    train_df['item_id'] = train_df['item_id'].map(item_mapping)
    train_df['sale_center_id'] = train_df['sale_center_id'].map(sale_center_mapping)
    gc.collect()
    print(train_df.head())
    
    ########## train model and predict ############
    
    # train purchase model
    params = {
         #'num_leaves': 30,
         #'min_data_in_leaf': 5, 
         'objective':'binary',
         'max_depth': -1,
         'learning_rate': 0.05,
         "boosting": "gbdt",
         "feature_fraction": 0.7,
         "bagging_seed": 42,
         "verbosity": 1,
         "nthread": -1,
         "random_state": 42,
         #'early_stopping':50
    }
    pur_model,pur_feats = train_pur_model(params, train_df, n=n, val=True, num_iter=20000)
    
    
    # train value model
    params = {
         #'num_leaves': 30,
         #'min_data_in_leaf': 5, 
         'objective':'mse',
         'max_depth': -1,
         'learning_rate': 0.01,
         "boosting": "gbdt",
         "feature_fraction": 0.7,
         "bagging_seed": 42,
         "verbosity": 1,
         "nthread": -1,
         "random_state": 1,
         #'early_stopping':200
    }

    val_model,val_feats = train_val_model(params, train_df, n=n, val=True, num_iter=20000)
    
    # make testset and predict
    max_date = pad_df.born_date_week.max()
    items = pad_df.item_id.unique()
    sale_centers = pad_df.sale_center_id.unique()
    max_rolling_win=5
    test_df = pad_df[pad_df.born_date_week>=(max_date-datetime.timedelta(days=(max_rolling_win+n)*7))] 
    feat_cols = [x for x in test_df.columns if x not in ['item_id','sale_center_id','born_date_week','time']+not_ts_feat]
    test_df = create_rolling_feat(test_df,feat_cols,win=5)
    test_df['predict_date'] = test_df['born_date_week'] + datetime.timedelta(days=n*7)
    test_df = create_holiday_feat(holiday_df,test_df,'predict_date')
    test_df = create_date_feat(test_df,'predict_date')
    test_df = create_expected_historical_feat(holiday_df,test_df,'predict_date')
    test_df['item_id'] = test_df['item_id'].map(item_mapping)
    test_df['sale_center_id'] = test_df['sale_center_id'].map(sale_center_mapping)
    test_df['will_purchase'] = pur_model.predict(test_df[pur_feats],num_iteration=pur_model.best_iteration_)
    test_df['val_pred'] = np.exp(val_model.predict(test_df[val_feats], num_iteration=val_model.best_iteration_))-100
    
    # tranform results
    sub_df = test_df[test_df.predict_date>max_date][['item_id','sale_center_id','predict_date','will_purchase','val_pred']]
    sub_df['prediction'] = (sub_df['will_purchase']*sub_df['val_pred']).astype('int')
    sub_df['item_id'] = sub_df['item_id'].map(inv_item_mapping)
    sub_df['sale_center_id'] = sub_df['sale_center_id'].map(inv_sale_center_mapping)
    sub_df = pd.pivot_table(sub_df, columns='predict_date', values='prediction', index=['item_id','sale_center_id'])
    sub_df = sub_df.reset_index()
    
    return sub_df


if __name__ == '__main__':
    # data_path = 'e://test//co_co_line.parquet'
    data_path = 'e://test/co_co_line.csv'

    # from pyspark.sql import SparkSession
    # spark=SparkSession.builder.appName("sales forecast")\
    #             .master("local[*]")\
    #             .getOrCreate()
    #             # .config("spark.sql.execution.arrow.enabled","true")\
    #             # .config("spark.kryoserializer.buffer","1024m")\
    #             # .getOrCreate()
    #
    # df=spark.read.parquet(data_path)
    # df.printSchema()
    # pd_df=df.toPandas()
    #
    # print(pd_df.dtypes)
    df = pd.read_csv(data_path)
    df=df[["cust_id", "co_num", "line_num", "item_id", "qty_need", "qty_ord", "qty_rsn", "price", "amt", "born_date","sale_center_id"]]
    print(df.dtypes)
    df["born_date"]=df["born_date"].astype("int")
    df["born_date"] = df["born_date"].astype("str")
    # print(df[df["qty_ord"]<0])
    print(df.dtypes)
    print(df.head())


    # 需要co_co_line的
    # cust_id           object
    # co_num            object
    # line_num          float64
    # item_id           object
    # qty_need          object
    # qty_ord           object
    # qty_rsn           object
    # price             object
    # amt               object
    # born_date         object
    # sale_center_id    object
    n = 2
    sub_df = predict(df,n)
    # print(sub_df)
    sub_df.to_csv("e://test/"+'zhuzhou_qtyord_'+str(n)+'_'+datetime.datetime.strftime(datetime.datetime.today(), format='%Y%m%d')+'.csv',index=False,sep=',')
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    