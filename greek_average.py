import pickle
import pandas as pd
import numpy as np
from datetime import timedelta
from scipy.stats import ttest_ind


df = pd.read_pickle('merged_df2022-8-24-2024-8-7.pkl')



# 計算價平差距
df['價平差距'] = df['參考s'] - df['履約價格']




# 定義篩選管道
def filter_pipeline(df, start_time, end_time):
    # 根據指定的時間段篩選數據
    df = df[(df['差異'] <= start_time) & (df['差異'] >= end_time)]
    
    # # 篩選買賣權為P的資料
    # df = df[df['買賣權別'] == 'C']
   
    # # 篩選價平差距為 +50 的資料
    # df = df[df['價平差距'] == 50]

    return df

# 使用篩選後的數據
filtered_df = filter_pipeline(df, timedelta(days=0, hours=4, minutes=45), timedelta(days=0, hours=0, minutes=0))

filtered_df['時間'] = filtered_df['時間'].dt.strftime('%H:%M:%S')
grouped_df = filtered_df.groupby(['時間','價平檔位','買賣權別']).agg(
    {
    '選擇權_收盤價': 'mean',
    'delta': 'mean',
    'gamma': 'mean',
    'theta': 'mean',
    'vega': 'mean',
})

filtered_grouped_df = grouped_df[(grouped_df.index.get_level_values('價平檔位') == -100) & 
                                 (grouped_df.index.get_level_values('買賣權別') == 'C')]



groupfiltered_grouped_dfed_df.to_csv('filtered_grouped_df.csv',encoding='utf-8-sig')




#%%



# 計算價平差距
df['價平差距'] = df['參考s'] - df['履約價格']




# 定義篩選管道
def filter_pipeline(df, start_time, end_time):
    # 根據指定的時間段篩選數據
    df = df[(df['差異'] <= start_time) & (df['差異'] >= end_time)]
    
    # # 篩選買賣權為P的資料
    # df = df[df['買賣權別'] == 'C']
   
    # # 篩選價平差距為 +50 的資料
    # df = df[df['價平差距'] == 50]

    return df

# 使用篩選後的數據
date_df = filter_pipeline(df, timedelta(days=0, hours=4, minutes=45), timedelta(days=0, hours=0, minutes=0))

date_df_new = date_df[date_df['時間'].dt.date==pd.to_datetime('2024-06-12').date()]

date_df_new.to_csv('date_df_new.csv',encoding='utf-8-sig')