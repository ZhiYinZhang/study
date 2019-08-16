# -*- coding: utf-8 -*-
"""
Created on Mon Jun 17 10:22:42 2019

@author: user
"""

### idea
# 1. predict if label is larger than 0
# 2. predict the value of label if label larger than 0


import lightgbm as lgb
import pandas as pd
import datetime

def train_pur_model(params,df,n,val,num_iter,sep_date='2019-04-01'):
    
    # predict if label is larger than 0, which means if the qty_rsn is 0 or not
    
    label = 'purchase_label'
    min_date = df.born_date_week.min()
    start_date = min_date + pd.Timedelta(str(7*n)+'d')
    pur_feats = [x for x in df if x not in ['born_date_week','label','log_label',
                                                'pred','purchase_label','will_purchase',
                                                'val_pred','prediction']]
    
    X_y_df = df.copy()
    
    if val:

        train_X, train_y = X_y_df[(X_y_df.born_date_week<sep_date)&(X_y_df.born_date_week>=start_date)][pur_feats],\
                    X_y_df[(X_y_df.born_date_week<sep_date)&(X_y_df.born_date_week>=start_date)][label]
        val_X, val_y = X_y_df[X_y_df.born_date_week>=sep_date][pur_feats], X_y_df[X_y_df.born_date_week>=sep_date][label]
        model_purchase = lgb.LGBMClassifier(**params, n_estimators=num_iter, n_jobs=-1)
        model_purchase.fit(train_X, train_y, eval_set=[(val_X, val_y)], eval_metric='binary_error',verbose=1,early_stopping_rounds=20,
                                                       categorical_feature=['item_id',    
                                                                            #'month',
                                                                            #'weekofyear',
                                                                            #'weekinmonth',
                                                                            'fut1_holiday_type',
                                                                            'his1_holiday_type'
                                                                           ])
        print('-'*50,'\n',model_purchase.best_iteration_,'\n','-'*50)
        num_iter = model_purchase.best_iteration_ + 10
    
    train_X, train_y = X_y_df[(X_y_df.born_date_week>=start_date)][pur_feats],\
                X_y_df[(X_y_df.born_date_week>=start_date)][label]
    if 'early_stopping' in params.keys():
        params.pop('early_stopping')
    model_purchase = lgb.LGBMClassifier(**params, n_estimators=num_iter, n_jobs=-1, verbose=1)       
    model_purchase.fit(train_X, train_y, verbose=1,
                       categorical_feature=['item_id',
                                    #'month',
                                    #'weekofyear',
                                    #'weekinmonth',
                                    'fut1_holiday_type',
                                    'his1_holiday_type'
                                   ])

    return model_purchase,pur_feats


def train_val_model(params,df,n,val,num_iter,sep_date='2019-04-01'):
    
    # predict the value of label if label larger than 0
    
    label = 'log_label'
    min_date = df.born_date_week.min()
    start_date = min_date + pd.Timedelta(str(7*n)+'d')
    val_feats = [x for x in df if x not in ['born_date_week','label','log_label',
                                                'pred','purchase_label','will_purchase',
                                                'val_pred','prediction']]
    val_X_y_df = df[(df['label']>0)&(df['born_date_week']>=start_date)]

    if val:
        train_X, train_y = val_X_y_df[val_X_y_df.born_date_week<sep_date][val_feats],\
                    val_X_y_df[val_X_y_df.born_date_week<sep_date][label]
        val_X, val_y = val_X_y_df[val_X_y_df.born_date_week>=sep_date][val_feats], \
                    val_X_y_df[val_X_y_df.born_date_week>=sep_date][label]
        model_val = lgb.LGBMRegressor(**params, n_estimators=num_iter, n_jobs=-1)
        model_val.fit(train_X, train_y, eval_set=[(val_X, val_y)], eval_metric='mse',verbose=1,early_stopping_rounds=200,
                                                  categorical_feature=['item_id',    
                                                                        #'month',
                                                                        #'weekofyear',
                                                                        #'weekinmonth',
                                                                        'fut1_holiday_type',
                                                                        'his1_holiday_type'
                                                                       ])
        print('-'*50,'\n',model_val.best_iteration_,'\n','-'*50)
        num_iter = model_val.best_iteration_ + 50
    
    train_X, train_y = val_X_y_df[val_feats],val_X_y_df[label]
    if 'early_stopping' in params.keys():
        params.pop('early_stopping')
    model_val = lgb.LGBMRegressor(**params, n_estimators=num_iter, n_jobs=-1, verbose=1)
    model_val.fit(train_X, train_y,verbose=1,
                  categorical_feature=['item_id',
                                       #'month',
                                       #'weekofyear',
                                       #'weekinmonth',
                                       'fut1_holiday_type',
                                       'his1_holiday_type'])

    return model_val,val_feats






















































