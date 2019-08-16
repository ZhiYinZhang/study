#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/6/21 9:58
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics.pairwise import pairwise_distances
from sklearn.metrics import jaccard_similarity_score
from sklearn.metrics.pairwise import cosine_similarity
from sklearn import preprocessing

def create_df():
    data=pd.read_csv('E:\pythonProject\jupyter/cigar_sales_need0.csv')
    return data

def categorical_features(data):
    data_categorical = data[['kind','product_type','yieldly_type','is_thin','sales_type','package_type']]
    return data_categorical

def num_features(data):
    data_num = data[['price','tar_cont','gas_nicotine','co_cont','cigar_long','m_long','box_num']]
    return data_num
#数值变量缺失值处理
def num_missing(data_num):
    data_num = ['price','tar_cont','gas_nicotine','co_cont','cigar_long','m_long','box_num']
    for col in data_num:
        data_num[col].fillna(data_num[col].median()[0],inplace=True)
    return data_num
#分类变量确实值处理
def categorical_missing(data_categorical):
    data_categorical = ['kind','product_type','yieldly_type','is_thin','sales_type','package_type']
    for col in data_categorical:
        data_categorical[col].fillna(data_categorical[col].mode()[0],inplace=True)
    return data_categorical

def MinMaxScaler(data_num):
    min_max_scaler = preprocessing.MinMaxScaler()
    data_num = min_max_scaler.fit_transform(data_num)
    data_num = pd.DataFrame(data_num)
    return data_num

def data_LabelEncoder(data_categorical):
    cols = ('kind','product_type','yieldly_type','is_thin','sales_type','package_type')
    for c in cols:
        lbl = LabelEncoder()
        lbl.fit(list(data_categorical[c].values))
        data_categorical[c] = lbl.transform(list(data_categorical[c].values))
    return data_categorical

def jaccard_similarity(data_categorical):
    # S_cos = 1 - pairwise_distances(f_encoded,f_encoded, metric='cosine')
    # S_jac = cosine2jaccard_similarity(S_cos)
    S_jac = 1 - pairwise_distances(data_categorical.astype('bool'), data_categorical.astype('bool'), metric='jaccard')
    return S_jac

def cos_similarity(data_num):
    S_cos = cosine_similarity(data_num)
    return S_cos

def similarity_all(S_jac,S_cos):
    similarity = 0.4*S_jac + 0.6*S_cos
    return similarity

def similarity_list1(S_jac,S_cos,data):
    S_list_1 = np.array([0,0]).reshape(1,2)
    S_list_2 = np.array([0,0]).reshape(1,2)
    similarity = similarity_all(S_jac,S_cos)
    m = similarity.shape[0]
    for i in  range(0,m):
        values = (data['item_id'])
        key = (similarity[i,:])
        d = list(zip(key,values))
        key1 = sorted(d, key=lambda x: x[0], reverse=True)[1][0]
        values1 = data['item_id'][i]
        data_1 = np.array([values1,key1]).reshape(1,2)
        data_1 = pd.DataFrame(data_1)
        S_list_1 = pd.DataFrame(S_list_1)
        S_list_1 = pd.concat([S_list_1,data_1],ignore_index=True)
        key2 = sorted(d, key=lambda x: x[0], reverse=True)[1][1]
        data_2 = np.array([values1,key2]).reshape(1,2)
        data_2 = pd.DataFrame(data_2)
        S_list_2 = pd.DataFrame(S_list_2)
        S_list_2 = pd.concat([S_list_2,data_2],ignore_index=True)
    S_list_1 = S_list_1.iloc[1:m+1,:]
    S_list_1.columns = ['item_id_1','similarity']
    S_list_2 = S_list_2.iloc[1:m+1,:]
    S_list_2.columns = ['item_id','s_item_id']
    S_list_all = pd.concat([S_list_2,S_list_1],axis=1)
    S_list_all = S_list_all[['item_id','s_item_id','similarity']]
    return S_list_all
def similarity_list(S_jac,S_cos,data):
    S_list_1 = pd.DataFrame()
    S_list_2 = pd.DataFrame()
    similarity = similarity_all(S_jac,S_cos)
    m = similarity.shape[0]
    for i in  range(0,m):
        values = (data['item_id'])
        key = (similarity[i,:])
        d = list(zip(key,values))
        for z in range(0,4):
            key1 = sorted(d, key=lambda x: x[0], reverse=True)[1:5][z][0]
            values1 = data['item_id'][i]
            data_1 = np.array([values1,key1]).reshape(1,2)
            data_1 = pd.DataFrame(data_1)
            S_list_1 = pd.DataFrame(S_list_1)
            S_list_1 = pd.concat([S_list_1,data_1],ignore_index=True).reset_index(drop=True)
        for j in range(0,4):
            key2 = sorted(d, key=lambda x: x[0], reverse=True)[1:5][j][1]
            data_2 = np.array([values1,key2]).reshape(1,2)
            data_2 = pd.DataFrame(data_2)
            S_list_2 = pd.DataFrame(S_list_2)
            S_list_2 = pd.concat([S_list_2,data_2],ignore_index=True)
    S_list_1.columns = ['item_id_1','similarity']
    S_list_2.columns = ['item_id','s_item_id']
    S_list_all = pd.concat([S_list_2,S_list_1],axis=1)
    S_list_all = S_list_all[['item_id','s_item_id','similarity']]
    return S_list_all
def get_similar_cigar(data):
    # data = create_df()
    # print(data.dtypes)
    data_categorical = categorical_features(data)
    data_num = num_features(data)
    #     data_categorical = categorical_missing(data_categorical)
    #     data_num = num_missing(data_categorical)
    data_categorical = data_LabelEncoder(data_categorical)
    data_num = MinMaxScaler(data_num)
    S_jac = jaccard_similarity(data_categorical)
    S_cos = cos_similarity(data_num)

    similarity = similarity_all(S_jac, S_cos)
    similar_cigar_list = similarity_list(S_jac, S_cos,data)

    return similar_cigar_list
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

    cigar_static=spark.read.csv("E:\\test\\tobacco_data\\static_cigar\\",header=True)

    pd_df=cigar_static.toPandas()

    print(pd_df.dtypes)
    similar_cigar_list=get_similar_cigar(pd_df)
    """
    item_id	        'gauge_id',
    price	        'ciga_price',
    item_name	    'gauge_name',
    kind	        'ciga_class',
    product_type	'ciga_type',
    yieldly_type	'ciga_origin_type',
    is_thin	        'is_thin',
    
    tar_cont	    'tar_cont',
    gas_nicotine	'gas_nicotine',
    co_cont	        'co_cont',
    cigar_long	    'ciga_long',
    m_long	        'm_long',
    package_type	'package_type',
    box_num	        'box_num',
    sales_type	    'sales_type'
    """
    print(similar_cigar_list)
    # df=spark.createDataFrame(similar_cigar_list)
