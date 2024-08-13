# %%%
import pandas as pd
import os
import numpy as np
import matplotlib.pyplot as plt
from util.BS_util import BS_formula

# 讀取加權股價指數



def read_index(year,month,day,kind):

    df_index = pd.read_csv(f'{year}{month}{day}{kind}.csv',encoding='utf-8-sig')
    df_index['發行量加權股價指數'] = df_index['發行量加權股價指數'].replace(',', '', regex=True).astype(float)


    # 將 df_index['時間'] 的日期部分替換為最大日期
    df_index['時間'] = year+":"+month+":"+day+":"+df_index['時間']
    df_index['時間'] = pd.to_datetime(df_index['時間'], format='%Y:%m:%d:%H:%M:%S')

    endtime = pd.Timestamp(
        year=df_index['時間'].max().year,
        month=df_index['時間'].max().month,
        day=df_index['時間'].max().day,
        hour=13,
        minute=30,
    )

    df_index['time'] = (endtime-df_index['時間'])/(endtime - df_index['時間'].min())/252

    # 取樣間隔為五筆資料
    df_index_new = df_index.iloc[::360]
    df_index_new = df_index_new[:-1]
    
    df_index_new = pd.concat([df_index_new,df_index.iloc[-13:]])

    df_index_new = df_index_new.iloc[:-12]
    

    df_index_new = df_index_new[['時間','發行量加權股價指數','time']]
    df_index = df_index[['時間','發行量加權股價指數','time']]

    return df_index,df_index_new

def read_tvix(year,month,day,kind):
    # TVIX
    df_tvix = pd.read_csv(
        f'data/202001歷史紀錄波動率指數_新VIX/tvix_{year}{month}{day}',
        encoding='Big5',
        header=None
    )
    df_tvix.drop([0,1],inplace=True)

    # 切割column
    df_tvix['日期']=df_tvix[0].str.split('\t').str[0]
    df_tvix['時間']=df_tvix[0].str.split('\t').str[1]
    df_tvix['tvix']=df_tvix[0].str.split('\t').str[4].astype(float)/100

    df_tvix['時間'] = df_tvix['時間'].apply(lambda x: '0' + x if len(x) == 7 else x)
    df_tvix['時間'] =(df_tvix['日期']  + df_tvix['時間']   )
    df_tvix['時間'] = pd.to_datetime(df_tvix['時間'], format='%Y%m%d%H%M%S%f')

    df_tvix.drop(columns=[0,'日期'],inplace=True)



    return df_tvix


year = '2020'
month = '01'
day = '02'
kind = 'TX1'

df_index,df_index_30 = read_index(year,month,day,kind)
df_tvix = read_tvix(year,month,day,kind)


df_merged = pd.merge(df_index_30, df_tvix, how='inner', on='時間')
price_range = {
    'min':round(df_merged['發行量加權股價指數'].mean()/100)*100-1000,
    'max':round(df_merged['發行量加權股價指數'].mean()/100)*100+1000,
    'step':100
                }



# # 取樣間隔為五筆資料
# sampled_data = df_gamma.iloc[::360]

def draw_gamma(df_merged,price_range):
    df_gamma = pd.DataFrame()
    for i in range(price_range['min'],price_range['max'],price_range['step']):
        # S = df_merged['發行量加權股價指數'][0]
        r = 0.00795

        # K = 16300
        # T = df_merged['time'][0]
        # sigma = 0.209223
        

        df_gamma[i] = df_merged.apply(
            lambda row: (
                BS_formula(
                    row['發行量加權股價指數'],
                    i,
                    r,
                    row['tvix'],
                    row['time'],
                )
                ).BS_gamma()[0]
            ,    axis=1,
        )

    df_gamma.index = df_merged['時間']

    # 設定圖表大小為 10x6
    plt.figure(figsize=(20, 6))
    # 迴圈遍歷 DataFrame 中的每一行，並繪製每一行的數據
    for idx, row in df_gamma.iterrows():
        plt.plot(df_gamma.columns, row, label=f'Row {idx}')


    # 添加標題和軸標籤
    plt.title(f'{year}{month}{day}-{kind}-Gamma by changing $S_0$')
    plt.xlabel('K')
    plt.ylabel('Gamma')


    # 添加圖例
    plt.legend()

    # 顯示圖表
    plt.show()

def draw_index(df_index):
    plt.figure(figsize=(20, 6))
    plt.plot(df_index['時間'],df_index['發行量加權股價指數'])
    plt.title(f'{year}{month}{day}-{kind}-index  $S_0$')
    plt.xlabel('time')
    plt.ylabel('index')
    plt.show()

def draw_delta(df_merged,price_range):
    df_delta = pd.DataFrame()
    for i in range(price_range['min'],price_range['max'],price_range['step']):
        # S = df_merged['發行量加權股價指數'][0]
        r = 0.00795

        # K = 16300
        # T = df_merged['time'][0]
        # sigma = 0.209223
        

        df_delta[i] = df_merged.apply(
            lambda row: (
                BS_formula(
                    row['發行量加權股價指數'],
                    i,
                    r,
                    row['tvix'],
                    row['time'],
                )
                ).BS_delta()[0]
            ,    axis=1,
        )

    df_delta.index = df_merged['時間']

    # 設定圖表大小為 10x6
    plt.figure(figsize=(20, 6))
    # 迴圈遍歷 DataFrame 中的每一行，並繪製每一行的數據
    for idx, row in df_delta.iterrows():
        plt.plot(df_delta.columns, row, label=f'Row {idx}')


    # 添加標題和軸標籤
    plt.title(f'{year}{month}{day}-{kind}-delta by changing $S_0$')
    plt.xlabel('K')
    plt.ylabel('delta')


    # 添加圖例
    plt.legend()

    # 顯示圖表
    plt.show()
draw_index(df_index)
draw_gamma(df_merged,price_range)
draw_delta(df_merged,price_range)

