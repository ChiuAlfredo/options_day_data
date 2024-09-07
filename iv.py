import os
from datetime import datetime, timedelta,time

from functools import reduce

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pymongo
import warnings

from util.BS_util import BS_formula
from scipy.optimize import curve_fit
from back_test_util.data_util import read_option_price, read_future_price, read_index, buil_Data, read_end_date
from back_test_util.option_action import Option
# 忽略 SettingWithCopyWarning 警告
pd.options.mode.chained_assignment = None

# 忽略 PerformanceWarning 警告
pd.options.mode.use_inf_as_na = True

# 忽略 PerformanceWarning 警告
warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)




end_date_df = read_end_date()


all_data = pd.DataFrame()
for index, row in end_date_df.iterrows():
    # index = 1
    if index == 0:
        continue
    elif index >100:
        break
    # elif index <388:
    #     continue
    else:
        # Define the date range
        
        start_date = end_date_df['結算日'][index]+ timedelta(seconds=1)
        end_date = end_date_df['結算日'][index-1]
        
        print(index)
        print(start_date,end_date)

        # Parse the date strings into datetime objects
        # start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        # end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        
        week_kind = end_date_df['到期月份(週別)'][index-1]
        
        month_option_price_df = read_option_price(start_date,end_date ,week_kind) 

        month_future_price_df_raw = read_future_price(start_date,end_date) 

        month_index = read_index(start_date,end_date) 


        merged_df = buil_Data(month_option_price_df,month_future_price_df_raw,month_index,end_date_df)

        
    

        merged_df.sort_values(by='時間', inplace=True)
        
        
        all_data = pd.concat([all_data,merged_df],ignore_index=True)



all_data['價平差距'] =all_data['參考s'] - all_data['履約價格'] 
def calculate_bin(value):
    if value >= 0:
        return (value // 50 + 1) * 50
    else:
        return (value // 50) * 50

# 使用 apply 方法將這個函數應用到 '價平差距' 欄位
all_data['價平檔位'] = all_data['價平差距'].apply(calculate_bin)

all_data.to_pickle('merged_df2022-8-24-2024-8-7.pkl')


all_data = pd.read_pickle('merged_df2022-8-24-2024-8-7.pkl')
from datetime import datetime

# 定義開始和結束的日期時間
start_time = datetime.strptime('2024-08-15 9:30:00', '%Y-%m-%d %H:%M:%S')
end_time = datetime.strptime('2024-08-21 13:30:00', '%Y-%m-%d %H:%M:%S')

# 計算 timedelta
time_difference = end_time - start_time

# 打印結果
print(f"Time difference: {time_difference}") 

time_list = [time_difference, time_difference- timedelta(hours=4), time_difference- timedelta(days=1), time_difference- timedelta(days=1,hours=4), time_difference- timedelta(days=4), time_difference- timedelta(days=4,hours=4),time_difference- timedelta(days=5), time_difference- timedelta(days=5,hours=4),time_difference- timedelta(days=6), time_difference- timedelta(days=6,hours=3),time_difference- timedelta(days=6,hours=3,minutes=30),time_difference- timedelta(days=6,hours=3,minutes=45) ]

strike_list = all_data['價平檔位'].unique()

# 假設中心值是某個特定值，例如 10000
center_value = 0

# 篩選在 +-1000 範圍內的值
filtered_strike_list = sorted(strike_list[(strike_list >= center_value - 1000) & (strike_list <= center_value + 1000)])

# 篩選 DataFrame 中的 價平檔位 在 filtered_strike_list 中的行
filtered_data = all_data[all_data['價平檔位'].isin(filtered_strike_list)]

# 初始化 '時間分組' 欄位
filtered_data['時間分組'] = -1


new_df = pd.DataFrame(columns=['時間', '價平檔位', '平均iv','標準差iv'])
for index,time_diff in enumerate(time_list):
    # index =0
    # time_diff = time_list[0]
    filtered_data.loc[filtered_data['差異'] == time_diff, '時間分組'] = time_diff


filtered_data = filtered_data[filtered_data['時間分組'] != -1]



# 根據 strike_price 和 time_group 分組，計算 average_iv 和 std_iv
grouped_data = filtered_data.groupby(['價平檔位', '時間分組','買賣權別']).agg(
    average_iv=('iv', 'mean'),
    std_iv=('iv', 'std')
).reset_index()

grouped_data.to_csv('iv.csv',encoding='utf-8-sig')