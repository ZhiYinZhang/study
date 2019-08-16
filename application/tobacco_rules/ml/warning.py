import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import datetime
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
import numpy as np
import pandas as pd
from hdfs.client import InsecureClient
from hdfs import client

spark = SparkSession.builder.enableHiveSupport().getOrCreate()


#### utils functions
def get_hdfs_client():
    """
    :return: client of hdfs
    """
    hdfs_host = "http://10.72.59.89:50070"
    user = "entrobus"

    cli = client.InsecureClient(url=hdfs_host, user=user)
    return cli


def get_pd_DF(cli: InsecureClient, file_path, header):
    """
       读取hdfs上的csv文件，返回pandas的DataFrame
    :param cli: hdfs的InsecureClient
    :param file_path: hdfs的文件路径,相对InsecureClient里面设置的root路径
    :return:
    """
    with cli.read(file_path) as reader:
        df_pd = pd.read_csv(reader, header=header)
    return df_pd


def save_pd_DF(df_pd: pd.DataFrame, cli: InsecureClient, file_path):
    """
     将pandas的DataFrame写入hdfs  csv
    :param df_pd: pandas的DataFrame
    :param cli: hdfs的InsecureClient
    :param file_path: hdfs的文件路径,相对InsecureClient里面设置的root路径
    """
    with cli.write(hdfs_path=file_path, encoding='utf-8', overwrite=True) as writer:
        df_pd.to_csv(writer)


def get_recent_days_order(df, tm_col, lag):
    '''
    get orders of recent days

    args:
        df: dataset to get orders
        tm_col: column of time
        lag: include orders of last recent lag days 
    '''

    now = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(days=lag), format='%Y%m%d')
    df = df[df[tm_col] >= now]
    df = df.fillna(0)
    return df


def get_agg_order(df, gb_c, feat_c):
    '''
    get aggregation of orders

    args:
        df: dataset to get aggregation
        gb_c: columns to groupby
        feat_c: feature columns to perform aggregation
    '''

    df = df.groupBy(gb_c) \
        .agg(F.sum(feat_c)) \
        .fillna(0)
    return df


def pyspark_read_phoenix(table_name):
    spark = SparkSession.builder.appName("spark hbase") \
        .master("local[*]") \
        .getOrCreate()

    ret_df = spark.read.format("org.apache.phoenix.spark") \
        .option("table", table_name) \
        .option("zkUrl", "10.72.59.91:2181") \
        .load()

    return ret_df


def to_pandas_df(dfs):
    '''
    transform all spark dataframes to pandas dataframes

    args:
        dfs: datasets to be transformed
    '''

    for i, df in enumerate(dfs):
        if i == 0:
            res = df
        else:
            res = res.join(df, on='cust_id')
    # res.show(100)
    return res.repartition(1).toPandas()


def read_hive_data(day='20190601', com_id='011114306'):
    '''
    获取近30天订货量、天价烟订货量、零售户画像
    args:
        day: the datetime string of the first day
    return:
        co_co: 订货量
        co_co_line: 高价烟订货量
        gb_ccl: 各高价烟品规订货量
    '''
    # spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    spark.sql('use aistrong')

    co_cust = spark.sql('''select cust_id from DB2_DB2INST1_CO_CUST 
    where (status='01' or status='02') and com_id='{com_id}' and 
    dt=(select max(dt) from DB2_DB2INST1_CO_CUST)'''.format(com_id=com_id))
    co_co = spark.sql(
        "select cust_id,qty_sum,amt_sum,born_date from DB2_DB2INST1_CO_CO_01 where com_id='{com_id}' and born_date >={day}".format(
            com_id=com_id, day=day))
    co_co = co_co.join(co_cust[['cust_id']], on='cust_id', how='right')

    co_co_line = spark.sql('''select co_num,item_id,qty_ord,price,born_date,cust_id from DB2_DB2INST1_CO_CO_LINE 
                            where com_id='{com_id}' and born_date>={day}'''.format(com_id=com_id, day=day))
    hicls = spark.sql(
        "select item_id from DB2_DB2INST1_PLM_ITEM_COM where dt=(select max(dt) from DB2_DB2INST1_PLM_ITEM_COM) and price_trade>636 and com_id='{com_id}'".format(
            com_id=com_id))
    hicls = hicls.dropDuplicates()
    sgp = spark.sql(
        " select item_id from DB2_DB2INST1_SGP_ITEM_SPW where begin_date>=20190101 and begin_date<=20190701 and supply_way=10 and com_id='{com_id}'".format(
            com_id=com_id))
    sgp = sgp.dropDuplicates()
    hicls = hicls.join(sgp, on='item_id')
    co_co_line = co_co_line.join(hicls, on='item_id')

    gb_ccl = co_co_line.groupby(['cust_id', 'item_id']).sum('qty_ord')
    item_name = spark.sql(
        "select item_id,item_name from DB2_DB2INST1_PLM_ITEM where dt=(select max(dt) from DB2_DB2INST1_PLM_ITEM)")
    gb_ccl = gb_ccl[gb_ccl['sum(qty_ord)'] > 0].join(item_name, on='item_id').toPandas()
    gb_ccl.rename(columns={'sum(qty_ord)': 'qty_ord'}, inplace=True)

    return co_co, co_co_line, gb_ccl, co_cust


def create_agg_func(x_col, y_col, method):
    '''
    分组做线性回归，实际值偏离预测值3、4、5倍标准差为C、B、A级异常
    '''
    assert method in ['percentile', 'mean_std']

    def agg_func(x, method=method, x_col=x_col, y_col=y_col):

        ori_col = x.columns
        x = x.dropna()
        if len(x) < 100:
            return None

        reg = LinearRegression()
        reg.fit(x[[x_col]], x[y_col])
        pred = reg.predict(x[[x_col]])
        residual = x[y_col] - pred
        std_res = np.std(residual)
        m_res = np.mean(residual)

        if method == 'percentile':
            C0_threshold = pred - np.percentile(residual, 1)
            C1_threshold = pred + np.percentile(residual, 99)
            B0_threshold = pred - np.percentile(residual, 0.1)
            B1_threshold = pred + np.percentile(residual, 99.9)
            A0_threshold = pred - np.percentile(residual, 0.01)
            A1_threshold = pred + np.percentile(residual, 99.99)
        else:
            C0_threshold = pred - 3 * std_res
            C1_threshold = pred + 3 * std_res
            B0_threshold = pred - 4 * std_res
            B1_threshold = pred + 4 * std_res
            A0_threshold = pred - 5 * std_res
            A1_threshold = pred + 5 * std_res

        x['pred'] = pred.astype('int')
        x['residual'] = residual.astype('int')
        x['C1_threshold'] = C1_threshold
        x['B1_threshold'] = B1_threshold
        x['A1_threshold'] = A1_threshold
        x['warning_level_code'] = 0

        # avoid all-zero situation
        x.loc[x['C1_threshold'] < 1, 'C1_threshold'] = 1
        x.loc[x['C1_threshold'] >= x['B1_threshold'], 'B1_threshold'] = x.loc[x['C1_threshold'] >= x[
            'B1_threshold'], 'C1_threshold'] + 1
        x.loc[x['B1_threshold'] >= x['A1_threshold'], 'A1_threshold'] = x.loc[x['B1_threshold'] >= x[
            'A1_threshold'], 'B1_threshold'] + 1

        x.loc[(x[y_col] > x['C1_threshold']) & (x[y_col] <= x['B1_threshold']), 'warning_level_code'] = 'C1'
        x.loc[(x[y_col] > x['B1_threshold']) & (x[y_col] <= x['A1_threshold']), 'warning_level_code'] = 'B1'
        x.loc[(x[y_col] > x['A1_threshold']), 'warning_level_code'] = 'A1'

        x['std_residual'] = std_res
        x['rec_upper_bound'] = (x['pred'] + 1.3 * x['std_residual']).astype('int')
        x['rec_lower_bound'] = (x['pred'] - 1.3 * x['std_residual']).astype('int')

        if y_col in ['grade']:
            x['rec_lower_bound'] = x['rec_lower_bound'].apply(lambda x: np.maximum(x, 1))
        else:
            x['rec_lower_bound'] = x['rec_lower_bound'].apply(lambda x: np.maximum(x, 0))
        x['rec_upper_bound'] = x.apply(lambda x: np.maximum(x['rec_upper_bound'], x['rec_lower_bound'] + 1), axis=1)

        ret_col = ['cust_id'] + [x for x in x.columns if x not in ori_col] + [y_col]
        res = x[ret_col]
        res.rename(columns={y_col: 'target_value'}, inplace=True)
        return res

    return agg_func


def get_warning_result(wl_dir, city='岳阳市', com_id='011114306', day='20190601',
                       cluster_dir='/user/entrobus/tobacco_data_630/cluster_result/'):
    cli = get_hdfs_client()
    wl = get_pd_DF(cli, wl_dir, header=None)
    wl.columns = ['com_id', 'whitelist', 'unknow']

    ############### get hive data #############
    co_co, co_co_line, gb_ccl, co_cust = read_hive_data(day=day, com_id=com_id)

    # aggregate orders in recent 30 days   
    cc_30_df = get_recent_days_order(co_co, 'born_date', 30)
    cc_30_agg_df = get_agg_order(cc_30_df, ['cust_id'], 'qty_sum')
    df = to_pandas_df([co_cust, cc_30_agg_df])

    ccl_30_df = get_recent_days_order(co_co_line, 'born_date', 30)
    ccl_30_agg_df = get_agg_order(ccl_30_df, ['cust_id'], 'qty_ord')
    ccl_df = to_pandas_df([co_cust, ccl_30_agg_df])
    ccl_df.rename(columns={'sum(qty_ord)': 'highcls_qty_ord'}, inplace=True)

    # get features dataframe from hdfs
    yy_df = pyspark_read_phoenix('TOBACCO.RETAIL')
    yy_df = yy_df[yy_df['CITY'] == city]
    yy_df = yy_df[['cust_id', 'sale_center_id', 'people_count', 'catering_cons_avg', 'grade']].toPandas()

    # read cluster result
    cluster_df = spark.read.csv(cluster_dir + '{city}'.format(city=city), header=True).toPandas()

    # get train df
    train_df = df.merge(yy_df, on='cust_id')
    train_df.rename(columns={'sum(qty_sum)': 'qty_sum'}, inplace=True)
    train_df = train_df.merge(cluster_df, on='cust_id')
    train_df = train_df.merge(ccl_df, on='cust_id', how='left')
    train_df['highcls_qty_ord'].fillna(0, inplace=True)
    # wl = pd.read_csv('whitelist.csv',dtype={'whitelist':'object'})
    train_df = train_df[train_df.cust_id.isin(wl.whitelist.values) == False]
    train_df = train_df[train_df.grade != 'ZZ']
    train_df[['qty_sum', 'people_count', 'grade', 'catering_cons_avg', 'highcls_qty_ord']] = \
        train_df[['qty_sum', 'people_count', 'grade', 'catering_cons_avg', 'highcls_qty_ord']].astype('float')

    train_df = train_df[train_df.cust_id.isin(wl.whitelist.values) == False]
    ########### get warning code ############
    res = []
    level_code_dict = {tuple(['people_count', 'qty_sum']): '001', tuple(['catering_cons_avg', 'grade']): '003',
                       tuple(['catering_cons_avg', 'highcls_qty_ord']): '004',
                       tuple(['grade', 'highcls_qty_ord']): '012'}
    code_map = {'C1': '001', 'B1': '002', 'A1': '003'}

    def get_threshold(x):
        if x['warning_level_code'] == 'C1':
            return x['C1_threshold']
        elif x['warning_level_code'] == 'B1':
            return x['B1_threshold']
        else:
            return x['A1_threshold']

    for i, cols in enumerate(
            [['people_count', 'qty_sum'], ['catering_cons_avg', 'grade'], ['catering_cons_avg', 'highcls_qty_ord'],
             ['grade', 'highcls_qty_ord']]):
        print(cols)
        agg_pred = create_agg_func(cols[0], cols[1], 'mean_std')
        if cols not in [['catering_cons_avg', 'highcls_qty_ord'], ['grade', 'highcls_qty_ord']]:
            tmp = train_df.groupby(['sale_center_id', 'cluster_index']).apply(lambda x: agg_pred(x)).reset_index().drop(
                'level_2', axis=1)
        elif cols in [['catering_cons_avg', 'highcls_qty_ord'], ['grade', 'highcls_qty_ord']]:
            tmp = train_df[train_df.highcls_qty_ord > 0].groupby(['sale_center_id']).apply(
                lambda x: agg_pred(x)).reset_index()
            tmp = tmp.drop('level_1', axis=1)

        tmp.drop(['std_residual', 'residual'], axis=1, inplace=True)
        tmp['classify_level1_code'] = 'YJFL' + level_code_dict[tuple(cols)]
        tmp['classify_level2_code'] = tmp['warning_level_code'].map(code_map)
        tmp['classify_level2_code'] = tmp['classify_level1_code'] + tmp['classify_level2_code']
        tmp['threshold'] = tmp.apply(lambda x: get_threshold(x), axis=1).astype('int')
        res.append(tmp[tmp.warning_level_code != 0][
                       ['sale_center_id', 'cust_id', 'classify_level1_code', 'classify_level2_code',
                        'warning_level_code',
                        'threshold', 'target_value', 'rec_upper_bound', 'rec_lower_bound']])
    res2 = pd.concat(res, axis=0)

    ########### transform warning code ##############
    cols = ['classify_id', 'cust_id', 'city', 'sale_center_id', 'classify_level1_code', 'classify_level2_code',
            'warning_level_code',
            'stream_avg_orders', 'retail_month_orders', 'retail_grade', 'retail_rim_cons', 'city_grade_cons_avg',
            'high_order_cons_ratio',
            'order_book_cons', 'order_price_cons', 'retail_month_price', 'retail_month_high', 'retail_month_order_book',
            'month_amount_ratio',
            'month_count_ratio', 'avg_orders_plus5', 'avg_orders_plus4', 'avg_orders_plus3', 'avg_orders_minu5',
            'avg_orders_minu4',
            'avg_orders_minu3', 'thread_hold', 'upper_bound', 'lower_bound', ]
    res = pd.DataFrame(columns=cols)
    new = res2.reset_index(drop=True)
    res['cust_id'] = new.cust_id.values
    res['city'] = city
    res['sale_center_id'] = new.sale_center_id.values
    res['classify_level1_code'] = new.classify_level1_code.values
    res['classify_level2_code'] = new.classify_level2_code.values
    res['warning_level_code'] = new.warning_level_code.values
    res['thread_hold'] = new.threshold.values
    res['upper_bound'] = new.rec_upper_bound.values
    res['lower_bound'] = new.rec_lower_bound.values
    res.loc[res.classify_level1_code == 'YJFL001', 'retail_month_orders'] = new.loc[
        new.classify_level1_code == 'YJFL001', 'target_value']
    res.loc[res.classify_level1_code == 'YJFL003', 'retail_grade'] = new.loc[
        new.classify_level1_code == 'YJFL003', 'target_value']
    res.loc[res.classify_level1_code == 'YJFL004', 'retail_month_high'] = new.loc[
        new.classify_level1_code == 'YJFL004', 'target_value']
    res.loc[res.classify_level1_code == 'YJFL012', 'retail_month_high'] = new.loc[
        new.classify_level1_code == 'YJFL012', 'target_value']
    cust = res[res.classify_level1_code.isin(['YJFL004', 'YJFL012'])]
    cust = cust[['cust_id', 'classify_level1_code']].merge(gb_ccl, on='cust_id')
    tmp = cust.groupby(['cust_id', 'item_name'], as_index=False).agg({'qty_ord': 'sum'})
    tmp = tmp[tmp.qty_ord > 0]
    tmp = tmp.merge(cust[['cust_id', 'classify_level1_code']], on='cust_id')

    def get_item_ord(x):
        item_name = x['item_name'].values
        item_ord = x['qty_ord'].values
        res = {}
        for n, o in zip(item_name, item_ord):
            #数据要float类型
            res[n] = float(o)
        return res

    tmp2 = tmp.groupby(['cust_id', 'classify_level1_code']).apply(lambda x: get_item_ord(x)).reset_index()
    tmp2.columns = ['cust_id', 'classify_level1_code', 'highprice_30days_order']
    res = res.merge(tmp2, on=['cust_id', 'classify_level1_code'], how='left')

    return res


if __name__ == '__main__':
    import json

    wl_dir = "/user/entrobus/zhangzy/dataset/whitelist"
    result = get_warning_result(wl_dir, city='岳阳市', com_id='011114306', day='20190601',
                                cluster_dir='/user/entrobus/tobacco_data_630/cluster_result/')

    print(result.head)
    print(result.dtypes)

    result["highprice_30days_order"] = result["highprice_30days_order"].apply(
        lambda x: json.dumps(x, ensure_ascii=False))

    df = spark.createDataFrame(result)
    df.show()

    df.write.csv("/user/entrobus/zhangzy/dataset/warning", header=True, mode="overwrite")