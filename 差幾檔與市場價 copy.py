import pandas as pd 
import matplotlib.pyplot as plt
from datetime import timedelta
import numpy as np
df_1 = pd.read_pickle('merged_df2022-1-01-2022-12-31.pkl')
df_2 = pd.read_pickle('merged_df2023-1-01-2023-12-31.pkl')

df = pd.concat([df_1,df_2],axis=0)



# 定義篩選管道
def filter_pipeline(df):

    # 篩選理論價格在145到155之間的數據
    # df = df[(df['bs_price'] >= 100) & (df['bs_price'] <= 150)]

    # 價平差距在450-550
    # df = df[(df['價平差距'] >= 450) & (df['價平差距'] <= 550)]
    
    # # 篩選T在0.00317和0.00316之間的數據
    # df = df[(df['T'] >= 0.00476) & (df['T'] <= 0.00377)]

    start_time = timedelta(days=1,hours=00,minutes=10)
    end_time = timedelta(days=0, hours=22, minutes=50)
    df = df[(df['差異'] <= start_time) & (df['差異'] >= end_time)]

    # 排除商品代號為TXO的數據
    df = df[df['商品代號_option'] != 'TXO']
    
    # 篩選買賣權為C的數據
    df = df[df['買賣權別'] == 'C']
    
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

df_filtered['價平差距']

df['價平差距'] =df['參考s'] - df['履約價格'] 
def calculate_bin(value):
    if value >= 0:
        return (value // 50 + 1) * 50
    else:
        return (value // 50) * 50

# 使用 apply 方法將這個函數應用到 '價平差距' 欄位
df_filtered['價平檔位'] = df_filtered['價平差距'].apply(calculate_bin)

# 假設 df_filtered 已經存在並且包含 '價平檔位' 和 '選擇權_收盤價' 欄位

# 選擇數值列
numeric_cols = df_filtered.select_dtypes(include='number').columns

# 根據 '價平檔位' 進行分組，計算每組的平均值和標準差
grouped = df_filtered.groupby('價平檔位')[numeric_cols].agg(['mean', 'std'])

# 重命名列名
grouped.columns = ['_'.join(col).strip() for col in grouped.columns.values]

# 選擇並保留 '選擇權_收盤價_mean' 和 '選擇權_收盤價_std' 這兩列
result = grouped[['選擇權_收盤價_mean', '選擇權_收盤價_std']]
result['+1std'] = result['選擇權_收盤價_mean'] + result['選擇權_收盤價_std']
result['-1std'] = result['選擇權_收盤價_mean'] - result['選擇權_收盤價_std']
result.to_csv('Call結算日前一天各檔位平均與標準差.csv',encoding='utf-8-sig')



