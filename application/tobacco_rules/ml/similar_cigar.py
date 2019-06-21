#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/6/21 9:58
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics.pairwise import pairwise_distances
from freediscovery.metrics import (cosine2jaccard_similarity,jaccard2cosine_similarity)
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

def similarity_list(data,similarity):
    S_list = np.array([0,0]).reshape(1,2)
    for i in  range(0,303):
        values = (data['item_id'])
        key = (similarity[i,:])
        d = list(zip(key,values))
        key1 = sorted(d, key=lambda x: x[0], reverse=True)[1][1]
        values1 = data['item_id'][i]
        data_1 = np.array([values1,key1]).reshape(1,2)
        data_1 = pd.DataFrame(data_1)
        S_list = pd.DataFrame(S_list)
        S_list = pd.concat([S_list,data_1],ignore_index=True)
    S_list = S_list.iloc[1:304,:]
    S_list.columns = ['item_id','s_item_id']
    return S_list

def get_similar_cigar():
    data = create_df()
    data_categorical = categorical_features(data)
    data_categorical = data_LabelEncoder(data_categorical)
    data_num = num_features(data)
    data_num = MinMaxScaler(data_num)
    S_jac =jaccard_similarity(data_categorical)
    S_cos = cos_similarity(data_num)
    similarity = similarity_all(S_jac,S_cos)
    similar_cigar_list = similarity_list(data,similarity)
    return similar_cigar_list
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

    similar_cigar_list=get_similar_cigar()

    print(similar_cigar_list)
    df=spark.createDataFrame(similar_cigar_list)
