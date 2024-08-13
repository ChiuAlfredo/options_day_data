import pandas as pd 
import matplotlib.pyplot as plt
from datetime import timedelta
import numpy as np
df_1 = pd.read_pickle('merged_df2022-1-01-2022-12-31.pkl')
df_2 = pd.read_pickle('merged_df2023-1-01-2023-12-31.pkl')

df = pd.concat([df_1,df_2],axis=0)

df['價平差距'] =df['參考s'] - df['履約價格'] 

# 定義篩選管道
def filter_pipeline(df):

    # 篩選理論價格在145到155之間的數據
    # df = df[(df['bs_price'] >= 100) & (df['bs_price'] <= 150)]

    # 價平差距在450-550
    # df = df[(df['價平差距'] >= 450) & (df['價平差距'] <= 550)]
    
    # # 篩選T在0.00317和0.00316之間的數據
    # df = df[(df['T'] >= 0.00476) & (df['T'] <= 0.00377)]

    start_time = timedelta(days=1,hours=00,minutes=5)
    end_time = timedelta(days=0, hours=22, minutes=55)
    df = df[(df['差異'] <= start_time) & (df['差異'] >= end_time)]

    # 排除商品代號為TXO的數據
    df = df[df['商品代號_option'] != 'TXO']
    
    # 篩選買賣權為C的數據
    df = df[df['買賣權別'] == 'P']
    
    return df

def draw_scatter(df,title):
    # 繪製散點圖
    plt.figure(figsize=(10, 10))
    plt.scatter(df['bs_price'], df['選擇權_收盤價'], alpha=0.5)

    # # 計算多次方趨勢線
    # degree = 3
    # z = np.polyfit(df_filtered['bs_price'], df_filtered['選擇權_收盤價'], degree)
    # p = np.poly1d(z)
    # x = np.linspace(df_filtered['bs_price'].min(), df_filtered['bs_price'].max(), 100)
    # plt.plot(x, p(x), "r--")

    plt.title(f'Scatter plot of bs_price vs market_price{title}')
    plt.xlabel('bs_price')
    plt.ylabel('market_price')
    plt.grid(True)
    plt.axis('square')
    plt.xlim(0,100)
    plt.ylim(0,100)
    plt.show()

# 應用篩選管道
df_filtered = filter_pipeline(df)
df_filtered['差價率'] =  df_filtered['選擇權_收盤價']/df_filtered['bs_price']

df_filtered = df_filtered[df_filtered['bs_price'] < 100]
df_bottom = df_filtered[(df_filtered['差價率'] < 0.8)]
df_middle= df_filtered[(df_filtered['差價率'] < 1.2) & (df_filtered['差價率'] > 0.8)]
df_top = df_filtered[ (df_filtered['差價率'] > 1.2)]

draw_scatter(df_filtered,'')
draw_scatter(df_bottom,' market/bs<0.8')
draw_scatter(df_middle,' 0.8<market/bs<1.2')
draw_scatter(df_top,' 1.2<market')

# df_top = df_filtered[df_filtered['差價'] >40 ]

# df_top_top = df_filtered[df_filtered['差價'] >100 ]
