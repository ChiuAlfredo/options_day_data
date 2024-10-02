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


hedge_df = pd.read_csv('filtered_grouped_df.csv')



move_stop_loss =0.5
fix_stop_loss = 0.1
moneyness = 3
# C_itm_50 = 0
# C_itm_100 = -1
# P_itm_50 = 1
inintial_cash = 1000
global profit_total
profit_total = 0
CorP = 'C'
hedge_frequency = 10


# record_df = pd.DataFrame(columns=['時間','BorS','履約價','CorP','價格','數量','手續費','損益','損益累計'])


record_list =[]

    
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
    # index = 1
    if index == 0:
        continue
    elif index >10:
        break
    # elif index <388:
    #     continue
    else:
        # Define the date range
        
        
        
        
        # start_date = end_date_df['結算日'][index]+ timedelta(seconds=1)
        # 只看結算
        start_date = end_date_df['結算日'][index-1]-  timedelta(days=0, hours=4, minutes=45)
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
        n=3
        waiting_time =60
        out_action=False
        
        option = Option()
        fix_strike =0
        for future_index, future_row in month_future_price_df_raw.iterrows():
            # future_index = 15
            # future_row = month_future_price_df_raw.iloc[future_index]
            
            if (future_row['時間'].strftime("%Y-%m-%d") == end_date.strftime("%Y-%m-%d")) & (future_row['時間'].hour >= 9):
                print(future_row['時間'])
                print(future_index)

                
                # 買入點：當時間為9點，且沒有部位，且還有交易次數
                
                if out_action==True:
                    waiting_time -=1
                    
                if waiting_time ==0:
                    out_action = False
                    waiting_time =60
                
                if (future_row['時間'].hour == 9) & (future_row['時間'].minute >= 0) & (option.position_list['option']==[])& (n>0):
                    choose_strike = find_most_itm_option(future_price = future_row['收盤價'])+moneyness*50
                    option_row =  filter_merged_df(merged_df, 時間=future_row['時間'], 履約價格=choose_strike, 買賣權別=CorP)
                    if option_row.empty:
                            continue

                    price = option_row['選擇權_收盤價'].values[0]
                    delta = option_row['delta'].values[0]
                    hege_delta = hedge_df[(hedge_df['時間']==future_row['時間'].strftime('%H:%M:%S')) & (hedge_df['價平檔位']==-moneyness*50)& (hedge_df['買賣權別']==CorP)]['delta'].values[0]
                    future_delta  = -hege_delta+delta
                    if delta<0.5 and option.position_list['option']==[] and option_row['iv'].values[0]>:
                        # out_action = True
                        option.option_trade(future_row['時間'],'S',CorP,choose_strike,price,1,0,position_action='open',record_dict={'delta':delta})
                        
                    if future_delta>=0 and option.position_list['future']['B']=={}:
                        # future_delta=1
                        option.future_trade(future_row['時間'],'B',future_row['收盤價'],future_delta,0,'open',record_dict={'option_detla':delta,'hedge_delta':hege_delta})
                    
                    n-=1
                
                fix_strike = choose_strike
                
                
                if (future_row['時間'].hour == 12) & (future_row['時間'].minute >= 40):
                    if option.position_list['future']['B']!={} :
                        option.future_trade(future_row['時間'],'S',position['現價'],position['數量'],0,position_action='close')
                    if option.position_list['option']!=[]:
                
                        # 更新position_list
                        for position in option.position_list['option']:
                            option.option_trade(future_row['時間'],'B',CorP,position['履約價'],position['現價'],1,0,position_action='close')
                    

                # 有選擇權部位
                if option.position_list['option']!=[]:
                
                    # 更新position_list
                    for position in option.position_list['option']:
                        # position =  option.position_list['option'][0]
                        
                        option_row =  filter_merged_df(merged_df, 時間=future_row['時間'], 履約價格=position['履約價'], 買賣權別=CorP)
                        if option_row.empty:
                            # 13:29就算沒有也要平倉
                            
                                continue
           
                        else:
                            price = option_row['選擇權_收盤價'].values[0]
                            delta = option_row['delta'].values[0]
                            hege_delta = hedge_df[(hedge_df['時間']==future_row['時間'].strftime('%H:%M:%S')) & (hedge_df['價平檔位']==-moneyness*50)& (hedge_df['買賣權別']==CorP)]['delta'].values[0]
                            future_delta  = -hege_delta+delta
                            
                            position['現價'] = option_row['選擇權_收盤價'].values[0]
                        # position['現價'] = option_row['選擇權_收盤價'].values[0]
                        

                        # # #TODO 0.5就出場
                        # if option_row['delta'].values[0]>0.5:
                        #     option.option_trade(future_row['時間'],'B',CorP,position['履約價'],position['現價'],1,0,position_action='close',record_dict={'delta':delta})
                        
                        
                            
                            
                        
                if option.position_list['future']['B']!={} :
                    # 更新position_list
                    # for position in option.position_list['future']:
                    position =  option.position_list['future']['B']
                                            
                    position['現價'] = future_row['收盤價']
                    
                    option_row =  filter_merged_df(merged_df, 時間=future_row['時間'], 履約價格=fix_strike, 買賣權別=CorP)
                    if option_row.empty:
                        continue
                    else:
                        price = option_row['選擇權_收盤價'].values[0]
                        delta = option_row['delta'].values[0]
                        hege_delta = hedge_df[(hedge_df['時間']==future_row['時間'].strftime('%H:%M:%S')) & (hedge_df['價平檔位']==-moneyness*50)& (hedge_df['買賣權別']==CorP)]['delta'].values[0]
                        future_delta  = -hege_delta+delta
                    
                    if future_index%hedge_frequency==0:
                    
                        if (future_delta>= 0) :
                            if future_delta>position['數量']:
                                option.future_trade(future_row['時間'],'B',position['現價'],future_delta-position['數量'],0,position_action='open',record_dict={'option_detla':delta,'hedge_delta':hege_delta})
                                
                            elif future_delta<position['數量']:
                                option.future_trade(future_row['時間'],'S',position['現價'],position['數量']-future_delta,0,position_action='close',record_dict={'option_detla':delta,'hedge_delta':hege_delta})
                                
                                
                        elif (future_delta<0) and (future_delta-position['數量'])>0:
                            option.future_trade(future_row['時間'],'S',position['現價'],position['數量'],0,position_action='close',record_dict={'option_detla':delta,'hedge_delta':hege_delta})
                            
                            
                    
                                    
                        
                        
                        
                    
        
        record_dict = {
            '日期':start_date.strftime('%Y-%m-%d'),
            '加權指數':month_index,
            '期貨':month_future_price_df_raw,
            '選擇權紀錄':option.record_list['option'],
            '期貨紀錄':option.record_list['future']
        }         
        record_list.append(record_dict)

profit_list = []
for index,record in enumerate(record_list):
    profit = 0
    # record = record_list[1]
    if record['期貨紀錄'] != []:
        profit +=record['期貨紀錄'][-1]['損益累計']
    if record['選擇權紀錄'] != []:
        profit +=record['選擇權紀錄'][-1]['損益累計']

    profit_list.append(profit)

profit_list = [float(profit) for profit in profit_list]

# 統計
statistic_data_dict ={
    '獲利次數':sum(1 for profit in profit_list if profit > 0),
    '虧損次數':sum(1 for profit in profit_list if profit <= 0),
    '獲利金額':sum(profit for profit in profit_list if profit > 0),
    '虧損金額':sum(profit for profit in profit_list if profit <=0),
}

statistic_dict = {
    '期間':f'{record_list[0]["日期"]}~{record_list[-1]["日期"]}',
    '總淨利':statistic_data_dict['獲利金額']+statistic_data_dict['虧損金額'],
    '毛利':statistic_data_dict['獲利金額'],
    '毛損失':statistic_data_dict['虧損金額'],
    '總交易天數':len(profit_list),
    '勝率':statistic_data_dict['獲利次數']/(statistic_data_dict['獲利次數']+statistic_data_dict['虧損次數']),
    
    '成功筆數':statistic_data_dict['獲利次數'],
    '失敗筆數':statistic_data_dict['虧損次數'],
    '最大獲利交易':max(profit_list),
    '最大虧損交易':min(profit_list),
    '成功交易平均獲利':statistic_data_dict['獲利金額']/statistic_data_dict['獲利次數'],
    '失敗交易平均虧損':statistic_data_dict['虧損金額']/statistic_data_dict['虧損次數'],
    '平均獲利/平均虧損':(statistic_data_dict['獲利金額']/statistic_data_dict['獲利次數'])/(statistic_data_dict['虧損金額']/statistic_data_dict['虧損次數']),
    '平均每筆交易盈虧':(statistic_data_dict['獲利金額']+statistic_data_dict['虧損金額'])/(statistic_data_dict['獲利次數']+statistic_data_dict['虧損次數']),
}

# 將 record_list 轉換為 DataFrame
with pd.ExcelWriter(f'record_list_double_short_{hedge_frequency}_{record_list[0]["日期"]}-{record_list[-1]["日期"]}.xlsx') as writer:
    statistic_df = pd.DataFrame(statistic_dict, index=[0])
    statistic_df.transpose().to_excel(writer, sheet_name='統計', header=False)
    for record in record_list:
        # record = record_list[0]
        # 將每個記錄轉換為 DataFrame
        df = pd.DataFrame()
        if record['期貨紀錄'] == []:
            df_future = pd.DataFrame(columns=['時間_期貨紀錄','收盤價_期貨紀錄','數量_期貨紀錄','手續費_期貨紀錄','損益_期貨紀錄','損益累計_期貨紀錄'])
        else:
            df_future = pd.DataFrame(record['期貨紀錄']).add_suffix('_期貨紀錄')
        
        if record['選擇權紀錄'] == []:
            df_option = pd.DataFrame(columns=['時間_選擇權紀錄','BorS_選擇權紀錄','倉位狀況_選擇權紀錄','履約價_選擇權紀錄','CorP_選擇權紀錄','價格_選擇權紀錄','數量_選擇權紀錄','手續費_選擇權紀錄','損益_選擇權紀錄','損益累計_選擇權紀錄'])
        else:
            df_option = pd.DataFrame(record['選擇權紀錄']).add_suffix('_選擇權紀錄')
            
        
        df = pd.merge(record['加權指數'],record['期貨'], left_on='分鐘時間',right_on='時間', how='inner', suffixes=('_加權指數', '_期貨'))
        df = df[['時間','收盤價_加權指數','收盤價_期貨']]
        df = pd.merge(df,df_option, left_on='時間',right_on='時間_選擇權紀錄', how='left', suffixes=('', '_選擇權紀錄'))
        df = pd.merge(df,df_future, left_on='時間',right_on='時間_期貨紀錄', how='left', suffixes=('', '_期貨紀錄'))
        
        df.sort_values(by='時間', inplace=True)
        
        # 使用日期作為分頁名稱
        sheet_name = record['日期']
        
        
        # 將 DataFrame 寫入 Excel 分頁
        df.to_excel(writer, sheet_name=sheet_name, index=False)

print("record_list 已保存為 record_list.xlsx")



