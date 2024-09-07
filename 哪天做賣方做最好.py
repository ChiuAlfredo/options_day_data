import pandas as pd 
import matplotlib.pyplot as plt
from datetime import timedelta
import numpy as np
# df_1 = pd.read_pickle('merged_df2022-1-01-2022-12-31.pkl')
df_2 = pd.read_pickle('merged_df2023-1-01-2023-12-31.pkl')

df = pd.concat([df_2],axis=0)

df['價平差距'] =df['參考s'] - df['履約價格'] 
df['theta'] = df['theta']/252


df['差價率'] =  df['選擇權_收盤價']/df['bs_price']

df['外在價值'] = df['選擇權_收盤價'] - df['價平差距']


def calculate_bin(value):
    if value >= 0:
        return (value // 50 + 1) * 50
    else:
        return (value // 50) * 50

# 使用 apply 方法將這個函數應用到 '價平差距' 欄位
df['價平檔位'] = df['價平差距'].apply(calculate_bin)

# 定義篩選管道
def filter_pipeline(df, start_time, end_time):

    # 篩選理論價格在145到155之間的數據
    # df = df[(df['bs_price'] >= 100) & (df['bs_price'] <= 150)]

    # 價平差距在450-550
    # df = df[(df['價平差距'] >= 450) & (df['價平差距'] <= 550)]
    
    # # 篩選T在0.00317和0.00316之間的數據
    # df = df[(df['T'] >= 0.00476) & (df['T'] <= 0.00377)]

    df = df[(df['差異'] <= start_time) & (df['差異'] >= end_time)]

    # 排除商品代號為TXO的數據
    df = df[df['商品代號_option'] != 'TXO']
    
    # 篩選買賣權為C的數據
    df = df[df['買賣權別'] == 'C']
    
    return df

result = pd.DataFrame(columns=['價平檔位'])
for day in range(0,7):

    start_time = timedelta(days=day, hours=4, minutes=30)
    end_time = timedelta(days=day, hours=4, minutes=20)
    df_open = filter_pipeline(df, start_time, end_time)
    
    
    start_time = timedelta(days=day, hours=0, minutes=10)
    end_time = timedelta(days=day, hours=0, minutes=0)
    df_close = filter_pipeline(df, start_time, end_time)


    grouped_open = df_open.groupby('價平檔位')['外在價值'].agg(['mean'])

    grouped_close = df_close.groupby('價平檔位')['外在價值'].agg(['mean'])

    merged_df = pd.merge(grouped_open, grouped_close, on='價平檔位',suffixes=('_open', '_close'))

    merged_df[f'{day}_diff'] = merged_df['mean_open'] - merged_df['mean_close']

    result = pd.merge(result, merged_df[f'{day}_diff'], on='價平檔位', how='outer')


