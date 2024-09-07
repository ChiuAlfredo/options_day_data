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






move_stop_loss =0.5
fix_stop_loss = 0.1
moneyness = 5
# C_itm_50 = 0
# C_itm_100 = -1
# P_itm_50 = 1
inintial_cash = 1000
global profit_total
profit_total = 0
CorP = 'C'


# record_df = pd.DataFrame(columns=['時間','BorS','履約價','CorP','價格','數量','手續費','損益','損益累計'])

option = Option(inintial_cash)

    
def find_most_itm_option(future_price):
    itm_strike_price = future_price -future_price%50
    return itm_strike_price

def filter_merged_df(merged_df, **kwargs):
    """
    根據 kwargs 中的鍵和值來過濾 merged_df
    """
    condition = pd.Series([True] * len(merged_df))
    for key, value in kwargs.items():
        condition &= (merged_df[key] == value)
    return merged_df.loc[condition]
    
for index, row in end_date_df.iterrows():
    # index = 3
    if index == 0:
        continue
    elif index >10:
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

            
        # 交易次數
        n=1

        for future_index, future_row in month_future_price_df_raw.iterrows():
            # future_index = 7200
            # future_row = month_future_price_df_raw.iloc[future_index]
            
            if (future_row['時間'].strftime("%Y-%m-%d") == end_date.strftime("%Y-%m-%d")) & (future_row['時間'].hour >= 9):
                # print(future_row['時間'])
                # print(future_index)

                
                # 買入點：當時間為9點，且沒有部位，且還有交易次數
                if (future_row['時間'].hour == 9) & (future_row['時間'].minute >= 0) & (option.position_list==[])& (n>0):
                    
                    choose_strike = find_most_itm_option(future_price = future_row['收盤價'])+moneyness*50
                    option_row =  filter_merged_df(merged_df, 時間=future_row['時間'], 履約價格=choose_strike, 買賣權別=CorP)
                    if option_row.empty:
                        continue
                    else:
                        price = option_row['選擇權_收盤價'].values[0]
                    
                    print(future_row['收盤價'])
                    
                    
                    price, profit_total = option.buy_call(future_row['時間'],choose_strike,price,1,0)
                    n-=1
                
                # 有部位
                if option.position_list!=[]:
                    # 更新position_list
                    for position in option.position_list:
                        # position = position_list[0]
                        option_row =  filter_merged_df(merged_df, 時間=future_row['時間'], 履約價格=position['履約價'], 買賣權別=CorP)
                        # option_row = merged_df[(merged_df['時間']==future_row['時間']) &(merged_df['履約價格']==position['履約價']) & (merged_df['買賣權別']==CorP)]
                        if option_row.empty:
                            continue
                        else:
                            position['現價'] = option_row['選擇權_收盤價'].values[0]
                            
                            if position['現價'] > position['最高價']:
                                position['最高價'] = position['現價']
                            if position['現價'] < position['最低價']:
                                position['最低價'] = position['現價']
                            
                            
                            match position['BorS']:
                                case 'B':
                                    # 移動停利
                                    if  (position['現價'] >= (1-move_stop_loss)*position['價格']) :
                                        if  position['是否該賣'] == True:
                                            price, profit_total = option.sell_call(future_row['時間'],position['履約價'],position['現價'],1,0)
                                        else:
                                            position['是否該賣'] = True
                                            print(future_row['收盤價'])
                                        
                                    else:
                                        position['是否該賣'] = False
                                        
                                    # # 固定停損
                                    # if  (position['現價'] <= (1-fix_stop_loss)*position['價格']) :
                                    #     if  position['是否該賣'] == True:
                                    #         price, profit_total = option.sell_call(future_row['時間'],position['履約價'],position['現價'],1,0,)
                                    #     else:
                                    #         position['是否該賣'] = True
                                    #         print(future_row['收盤價'])
                                        
                                    # else:
                                    #     position['是否該賣'] = False
                                case 'S':
                                    if  (position['現價'] >= (1+move_stop_loss)*position['最低價']) :
                                        if  position['是否該賣'] == True:
                                            price, profit_total = option.sell_call(future_row['時間'],position['履約價'],position['現價'],1,0)
                                        else:
                                            position['是否該賣'] = True
                                            print(future_row['收盤價'])
                                        
                                    else:
                                        position['是否該賣'] = False
                                    
                                    
                                    
                            # # 移動停損
                            # # 要連續兩次才停損
                            # if  (position['現價'] <= (1-move_stop_loss)*position['最高價'])  :
                            #     if  position['是否該賣'] == True:
                            #         price, profit_total = option.sell_call(future_row['時間'],position['履約價'],position['現價'],1,0,)
                            #     else:
                            #         position['是否該賣'] = True
                            #         print(future_row['收盤價'])
                                
                            # else:
                            #     position['是否該賣'] = False
                                
                          
                                
                                
                            if (future_row['時間'].hour == 13) & (future_row['時間'].minute >= 25):
                                price,profit_total = option.sell_call(future_row['時間'],position['履約價'],position['現價'],1,0)
                            
                        
                    
record_df = pd.DataFrame(option.record_list)
record_df
# record_df.to_csv('record_價外五檔.csv', encoding='utf-8-sig')