import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import datetime
import os
import warnings
import phoenixdb
from phoenixdb import cursor
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import StandardScaler
from keras.models import *
from keras.layers import *
from keras.callbacks import *
from keras import backend as K
from keras.engine.topology import Layer
from sklearn.model_selection import train_test_split
from sklearn.cluster import KMeans

warnings.filterwarnings('ignore')


############ read dataset from hive

def read_hive(city_code,spark):
    '''
    取某个城市的零售户的co_co数据
    '''
    # spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    spark.sql('use aistrong')

    co_cust = spark.sql('''select cust_id from DB2_DB2INST1_CO_CUST 
        where (status='01' or status='02') and com_id=\'''' + city_code + '''\' and 
        dt=(select max(dt) from DB2_DB2INST1_CO_CUST)''')  # 取某个城市cust_id
    co_co_line = spark.sql(
        '''select co_num,item_id,qty_need,qty_ord,qty_rsn,price,amt,born_date,cust_id from DB2_DB2INST1_CO_CO_LINE''')
    co_co_line = co_co_line.join(co_cust[['cust_id']], on='cust_id', how='right')
    co_co = spark.sql("select cust_id,qty_sum,amt_sum,born_date from DB2_DB2INST1_CO_CO where dt<='2019-06-04' ")
    co_co = co_co.join(co_cust[['cust_id']], on='cust_id', how='right')
    return co_co, co_cust


def pandas_read_phoenix(cursor, sql_statement: str, batch=10):
    '''
    pandas read dataset from hdfs

    args:
        cursor:
        sql_statement:
        batch:
    '''

    cursor.execute(sql_statement)
    rows = cursor.fetchall()
    cols = list(rows[0].keys())
    df = pd.DataFrame(columns=cols)
    df_rows = []
    for row in rows:
        df_rows.append(row)
        if len(df_rows) == batch:
            df = df.append(df_rows)
            df_rows.clear()
    if len(df_rows) > 0:
        df = df.append(df_rows)
    return df


def pyspark_read_phoenix(table_name, city,spark):
    # spark = SparkSession.builder.appName("spark hbase") \
    #     .master("local[*]") \
    #     .getOrCreate()
    ret_df = spark.read.format("org.apache.phoenix.spark") \
        .option("table", table_name) \
        .option("zkUrl", "10.72.59.91:2181") \
        .load()
    # print('Done!')
    # ret_df = ret_df.filter(("city='岳阳市'")&(("status = '01'")|("status = '02'")))
    ret_df = ret_df[(ret_df['city'] == city) & ((ret_df['status'] == '01') | (ret_df['status'] == '02'))]
    ret_df = ret_df[['cust_id',
                     'abcode',
                     'order_way',
                     'periods',
                     'work_port',
                     'base_type',
                     'sale_scope',
                     'scope',
                     'com_chara',
                     'sale_large',
                     'rail_cust',
                     'area_type',
                     'multiple_shop',
                     'night_shop',
                     'consumer_group',
                     'consumer_attr',
                     'grade',
                     'catering_cons_count',
                     'convenient_trans_count',
                     'shopping_cons_count',
                     'entertainment_count',
                     'accommodation_avg',
                     'order_competitive_index',
                     'people_count']]
    return ret_df.toPandas()


def get_recent_days_order(df, tm_col, co_cust, lag, now=1):
    '''
    get orders of recent days

    args:
        df: dataset to get orders
        tm_col: column of time
        lag: include orders  of last recent lag days 
    '''
    if now:
        now = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(days=lag), format='%Y%m%d')
    else:
        now = datetime.datetime.strftime(datetime.datetime(2019, 6, 1) - datetime.timedelta(days=lag), format='%Y%m%d')
    print(now)
    df = df[df[tm_col] >= now]
    df = co_cust.select(F.col('cust_id')).join(df, on='cust_id', how='left')
    df = df.fillna(0)
    return df


def get_item_order_feat(df, gb_c, pivot_c, val_c):
    '''
    get item orders and pivot them from rows to columns

    args:
        df: dataset to get item orders
        gb_c: row index of the return dataframe
        pivot_c: the column to pivot
        val_c: the column of values of the pivoted column
    '''

    df = df.groupBy(gb_c) \
        .pivot(pivot_c) \
        .agg(F.sum(val_c)) \
        .fillna(0)
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


def get_price(df):
    '''
    get mean purchase price

    args:
        df: dataset to get mean purchase price
    '''

    df = df.select(F.col('cust_id'), F.col('sum(qty_sum)').alias('qty'), F.col('sum(amt_sum)').alias('amt'))
    df = df.withColumn('price', df['amt'] / df['qty'])
    return df


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


def label_encode_cat(df, cat_cols):
    '''
    label encode categorical columns

    args:
        df: dataset to perform label encoding
        cat_cols: categorical columns

    return:
        df: return dataframe
        emb_dim: embedding dimension
        lab_enc: label encoder dictionary
    '''

    emb_dim = {}
    lab_enc = {}
    df = df.copy()
    for col in cat_cols:
        encoder = LabelEncoder()
        df[col] = encoder.fit_transform(df[col])
        emb_dim[col] = int(len(encoder.classes_) + 1)
        lab_enc[col] = encoder
    return df, emb_dim, lab_enc


def build_encoder(cat_cols, num_cols, emb_dim):
    '''
    build encoder

    args:

    return:
        logits: last layer of encoder
        encoder: the model of encoder
    '''

    cat_inputs = []
    for cat in cat_cols:
        cat_inputs.append(Input(shape=[1], name=cat))

    cat_embs = []
    for i, cat in enumerate(cat_cols):
        cat_embs.append(Embedding(emb_dim[cat], 10)(cat_inputs[i]))

    # cat logits
    cat_logits = Concatenate(axis=1)([cat_emb for cat_emb in cat_embs])
    cat_prob = Conv1D(len(cat_cols) ** 2, len(cat_cols), activation='softmax')(cat_logits)
    cat_prob = Reshape((len(cat_cols), len(cat_cols)))(cat_prob)
    cat_logits = Dot(axes=(1, 1))([cat_logits, cat_prob])
    # cat_logits = Reshape((len(cat_cols)*10,))(cat_logits)
    cat_logits = Flatten()(cat_logits)
    cat_logits = Dense(int(len(num_cols)), activation='tanh')(cat_logits)

    # num logits
    num_inputs = Input(shape=(len(num_cols),), name='num')
    num_logits = Dense(int(len(num_cols)), activation='tanh')(num_inputs)

    # combine cat and num
    logits = Concatenate()([num_logits, cat_logits])
    logits = Dense(int(len(num_cols)), activation='linear')(logits)
    encoder = Model(inputs=[num_inputs] + cat_inputs, output=logits)
    return logits, encoder


def build_decoder(enc_out, cat_cols, num_cols, emb_dim):
    '''
    build decoder

    args:
        enc_out: the output layer of decoder

    return:
        ret_outs: the output layers of decoder
        losses: 
        loss_weights:
    '''

    cat_logits = Dense(int(len(num_cols)), activation='tanh')(enc_out)
    num_logits = Dense(int(len(num_cols)), activation='tanh')(enc_out)
    cat_outs = []
    losses = {}
    loss_weights = {}

    # multi outputs of cate columns
    for cat in cat_cols:
        _logit = Dense(10, activation='tanh')(cat_logits)
        cat_outs.append(Dense(emb_dim[cat], activation='softmax', name=cat + '_out')(_logit))
        losses[cat + '_out'] = 'sparse_categorical_crossentropy'
        loss_weights[cat + '_out'] = 1

    # single outputs of num columns
    num_outs = Dense(len(num_cols), name='num_out', activation='relu')(num_logits)

    # define the outputs, losses and loss weights
    losses['num_out'] = 'mse'
    loss_weights['num_out'] = len(num_cols)
    ret_outs = [num_outs] + cat_outs

    return ret_outs, losses, loss_weights


def get_input_output(df, num_cols, cat_cols):
    '''
    get inputs for AE

    args:
        df: dataframe from witch inputs derived

    return:
        X_in: inputs of AE
        X_out: outputs of AE
    '''

    # get num input
    X_num = df[num_cols].values
    X_in = {'num': X_num}
    X_out = {'num_out': X_num}
    # get cat input
    for cat in cat_cols:
        X_in[cat] = df[cat].values
        X_out[cat + '_out'] = df[cat].values
    return X_in, X_out


def main(city,phoenix_table,result_path,spark):
    city_dict = {'岳阳市': '011114306', '邵阳市': '011114305', '株洲市': '011114302'}

    ########## get features dataframe from hdfs
    # sql = '''
    #     select cust_id,
    #     abcode,
    #     order_way,
    #     periods,
    #     work_port,
    #     base_type,
    #     sale_scope,
    #     scope,
    #     com_chara,
    #     sale_large,
    #     rail_cust,
    #     area_type,
    #     multiple_shop,
    #     night_shop,
    #     consumer_group,
    #     consumer_attr,
    #     grade,
    #     catering_cons_count,
    #     convenient_trans_count,
    #     shopping_cons_count,
    #     entertainment_count,
    #     accommodation_avg,
    #     order_competitive_index,
    #     people_count
    #     from tobacco.retail where city='岳阳市' and (status='01' or status='02')
    #     '''
    # rent_info_avg,
    # morning_stream,
    # noon_stream,
    # night_stream,
    # weekend_stream,
    # mid_week_stream,
    # database_url = "http://10.72.32.26:8765"
    # conn = phoenixdb.connect(database_url, max_retries=3, autocommit=True)
    # cursor = conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)
    # yyfeat_df = pandas_read_phoenix(cursor, sql, batch=100)
    # yyfeat_df.columns = [x.lower() for x in yyfeat_df.columns]

    # spark = SparkSession.builder.appName("spark hbase") \
    #     .enableHiveSupport() \
    #     .master("local[*]") \
    #     .getOrCreate()
    # yyfeat_df = pyspark_read_phoenix("TOBACCO.RETAIL", city, spark)
    yyfeat_df = pyspark_read_phoenix(phoenix_table, city,spark)

    ########## modify null value of area_type column
    yyfeat_df['area_type'] = yyfeat_df['area_type'].apply(lambda x: x if x != 'NU' else 'NULL')

    ######### aggregate orders in recent 30 days
    try:
        city_code = city_dict[city]
    except:
        print('所输入城市不在city_dict中，合法城市为：\n', city_dict.keys())
        return 0
    co_co, co_cust = read_hive(city_code,spark)
    cc_30_df = get_recent_days_order(co_co, 'born_date', co_cust, 30, 0)
    # ccl_30_df = get_recent_days_order(co_co_line,'born_date',30)
    # ccl_30_item_df = get_item_order_feat(ccl_30_df, gb_c=['cust_id'], pivot_c='item_id', val_c='qty_ord')
    for i, c in enumerate(['qty_sum', 'amt_sum']):
        if i == 0:
            cc_30_agg_df = get_agg_order(cc_30_df, ['cust_id'], c)
        else:
            cc_30_agg_df = cc_30_agg_df.join(get_agg_order(cc_30_df, ['cust_id'], c), on='cust_id')
    cc_30_agg_p_df = get_price(cc_30_agg_df)
    df = to_pandas_df([co_cust, cc_30_agg_p_df])
    df = df.merge(yyfeat_df, left_on='cust_id', right_on='cust_id', how='inner')
    df['price'] = df['price'].fillna(0)

    ########## get columns with few null
    null_per = df.isnull().sum() / df.shape[0]
    not_null_col = null_per[null_per < 0.6].index
    num_cols = [x for x in ['qty', 'amt', 'price', 'catering_cons_count', 'convenient_trans_count',
                            'shopping_cons_count', 'entertainment_count', 'accommodation_avg',
                            'order_competitive_index', 'catering_cons_avg', 'people_count']
                if x in not_null_col]
    cat_cols = [x for x in not_null_col if x not in ['cust_id'] + num_cols]
    print('num_cols: ', num_cols)
    print('cat_cols: ', cat_cols)
    # return df
    for col in not_null_col:
        if df[col].isnull().sum() > 0:
            if col in cat_cols:
                df[col] = df[col].fillna('NULL')
            else:
                df[col] = df[col].apply(lambda x: str(x).replace('None', '0'))
                df[col] = df[col].fillna(0).astype('float')

    ######## label encode cate columns
    enc_df, emb_dim, lab_enc = label_encode_cat(df, cat_cols)

    ########## standardize num columns
    scaler = StandardScaler()
    enc_std_df = enc_df.copy()
    enc_std_df[num_cols] = scaler.fit_transform(enc_std_df[num_cols])

    ########## build AE
    enc_out, encoder = build_encoder(cat_cols, num_cols, emb_dim)
    dec_out, losses, loss_weights = build_decoder(enc_out, cat_cols, num_cols, emb_dim)
    ae = Model(inputs=encoder.get_input_at(0), outputs=dec_out)
    ae.compile(loss=losses, loss_weights=loss_weights, optimizer='adam')

    ########## train AE
    tr_df, val_df = train_test_split(enc_std_df, test_size=0.1, random_state=1)
    tr_in, tr_out = get_input_output(tr_df, num_cols, cat_cols)
    val_in, val_out = get_input_output(val_df, num_cols, cat_cols)
    es = EarlyStopping(monitor='val_loss', patience=20)
    rlr = ReduceLROnPlateau(monitor='val_loss', factor=0.5, patience=5, min_lr=0.0001)
    ae.fit(tr_in, tr_out, validation_data=[val_in, val_out], batch_size=500, epochs=1000, callbacks=[es, rlr],
           verbose=1)

    ########## predict encoder outout
    X_in, X_out = get_input_output(enc_std_df, num_cols, cat_cols)
    encoder_output = encoder.predict(X_in, batch_size=len(enc_std_df))

    ########## cluster and write result to hdfs
    df['encoder_output'] = list(encoder_output)
    cluster_df = df[['cust_id', 'encoder_output']]

    # hyperparamters
    N_CLUSTER = int(np.log(len(enc_std_df)))

    km = KMeans(N_CLUSTER)
    cluster_df['cluster_index'] = km.fit_predict(np.array(list(map(list, cluster_df.encoder_output.values))))

    result = spark.createDataFrame(cluster_df[['cust_id', 'cluster_index']])
    # file_path = '/user/entrobus/tobacco_data_630/cluster_result/' + city
    file_path=os.path.join(result_path,city)
    result.repartition(1).write.csv(path=file_path, header=True, sep=",", mode='overwrite')