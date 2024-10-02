import pickle
import pandas as pd
import numpy as np
from datetime import timedelta
from scipy.stats import ttest_ind
from back_test_util.data_util import read_option_price, read_future_price, read_index, buil_Data, read_end_date
from back_test_util.option_action import Option


start_date = '2022-01-01 08:45:00'
end_date = '2024-06-12 13:30:00'

end_date_df = read_end_date()


print(start_date,end_date)

# Parse the date strings into datetime objects
# start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
# end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

week_kind = end_date_df['到期月份(週別)'][index-1]

month_option_price_df = read_option_price(start_date,end_date ,week_kind) 

month_future_price_df_raw = read_future_price(start_date,end_date) 

month_index = read_index(start_date,end_date) 


merged_df = buil_Data(month_option_price_df,month_future_price_df_raw,month_index,end_date_df)




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
    'iv':'mean'
})

filtered_grouped_df = grouped_df[(grouped_df.index.get_level_values('價平檔位') == -100) & 
                                 (grouped_df.index.get_level_values('買賣權別') == 'C')]



filtered_grouped_df.to_csv('filtered_grouped_df.csv',encoding='utf-8-sig')




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