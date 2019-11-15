#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import pandas as pd
import numpy as np
import os
import datetime
# import happybase
# from happybase import Table


# rootdir = 'D:\\Jupyter\\data\\neiguan'
rootdir = 'e://test//ml//upload'
months = [str(i) for i in range(1, 13)]
years = [str(i) for i in range(2010, 2040)]

print("years",years)
print("months",months)

mon_list = []
first = datetime.date.today()

for i in range(6):
    first = first.replace(day=1) - datetime.timedelta(days=1)
    print(first)
    mon_list.append(first.strftime("%Y%m"))
print(mon_list)


def filter_lingshouhu(df, file_month, proloc):
    strlist = ['X', '\*', '999', '888', 'x', 'C', ' ']
    for i in strlist:
        bool = df['零售户'].str.contains(i)
        bool1 = bool.apply(lambda x : False if x else True)
        df = df[bool1]

    df['month'] = file_month
    df['shengbie'] = proloc
    return df

def statistics(df : pd.DataFrame):
    groupby01 = df.groupby('零售户')
    stat01 = groupby01.size()
    stat02 = pd.DataFrame(stat01[stat01 >= 15])
    # print(type(stat02))
    stat02.reset_index(inplace=True)
    stat02[['零售户']].to_csv(rootdir + '/../result/neiguan_ge_15.csv', header=True,index=False)
    # stat01['零售户'] = stat01.index.values
    # print(type(stat01['零售户']), stat01)

    groupby02 = df[['零售户', '案件录入号']]
    groupby02 = groupby02.drop_duplicates()
    groupby02 = groupby02.groupby('零售户')
    stat03 = groupby02.size()
    stat04 = pd.DataFrame(stat03[stat03 >= 2])
    stat04.reset_index(inplace=True)
    # stat04.to_csv(rootdir + '/../result/neiguan_ge_2.csv', header=True,index=False)
    stat04[['零售户']].to_csv(rootdir + '/../result/neiguan_ge_2.csv', header=True,index=False)
    print('...output file to folder result/ ')


def read_file(rootdir):
    filelist = os.listdir(rootdir)
    print("filelist:",filelist)
    output = pd.DataFrame()
    for i in filelist:
        if i.endswith('xls'):
            monloc = i.find('月')
            yearloc = i.find('年')
            print(yearloc,monloc,i[monloc-1],i[(yearloc-4):yearloc])

            if monloc != -1 and (i[monloc - 1] in months or i[(monloc-2):monloc] in months) and \
                    yearloc != -1 and i[(yearloc - 4) : yearloc] in years:

                file_month = i[yearloc + 1:monloc]
                if len(file_month) < 2:
                    file_month = '0%s'%(file_month)
                file_year = i[(yearloc - 4) : yearloc]

                file_month = '%s%s'%(file_year, file_month)
                if file_month not in mon_list:
                    continue
                # print(file_year,file_month)
                # file_month = i[monloc - 1:monloc + 1]
                #             print(i[monloc - 1])
                #             print(file_month)

                proloc = 'Unknown'
                inloc = i.find('省内')
                outloc = i.find('省外')
                if inloc != -1:
                    proloc = i[inloc: inloc + 2]
                if outloc != -1:
                    proloc = i[outloc: outloc + 2]
                print(os.path.join(rootdir,i))
                df = pd.read_excel(os.path.join(rootdir, i))
                # print(file_month, proloc, df.shape)

                df_filter = filter_lingshouhu(df, file_month, proloc)
                # print('after filter shape: ', df_filter.shape)
                #             df_filter.append(output)
                output = output.append(df_filter)
                # print('after append shape: ', output.shape)
    output.to_csv(rootdir + '/../result/consolidate.csv', header=True,index=False)
    statistics(output)
    return output


def pd_write_hbase(df:pd.DataFrame,cols:list):
    """
    :param df:  pandas DataFrame
    :param cols: 除row_key之外
    """

    hbase_host="10.72.59.90"
    hbase_pool_size=10

    host="10.72.32.26"
    table="member3"
    row="sale_center_id"
    family="column_A"

#    pool=happybase.ConnectionPool(host=hbase_host,size=hbase_pool_size)
#    with pool.connection() as conn:
#        table: Table = conn.table(table)
#        try:
#            with table.batch(batch_size=1000) as batch:
#                for x in range(len(df)):
#                    l = df.iloc[x][cols].values
#
#                    row_key=df.iloc[x][row] # row key
#
#                    data = {}
#                    for y in range(len(cols)):
#                        fly_col = f"{family}:{cols[y]}"
#                        data[fly_col] = str(l[y])
#                    batch.put(row=str(row_key), data=data)
#        except Exception as e:
#            print(e.args)
#
#

if __name__ == '__main__':
    read_file(rootdir)