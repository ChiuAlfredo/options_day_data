from datetime import timedelta
import pandas as pd
import warnings

from back_test_util.data_util import read_option_price, read_future_price, read_index, buil_Data, read_end_date
from back_test_util.option_action import Option
from back_test_util.record import Record
from concurrent.futures import ThreadPoolExecutor
import queue

# 忽略 SettingWithCopyWarning 警告
pd.options.mode.chained_assignment = None

# 忽略 PerformanceWarning 警告
pd.options.mode.use_inf_as_na = True

# 忽略 PerformanceWarning 警告
warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)




end_date_df = read_end_date()


hedge_df = pd.read_csv('filtered_grouped_df.csv')


hl_12_20_df = pd.read_csv('12點20天平均.csv')

hl_12 = hl_12_20_df['20天平均_1230-1255'].sum()/len(hl_12_20_df['20天平均_1230-1255'])


hl_12_20_df_index = pd.read_csv('12點20天平均_index.csv')

hl_12_index = hl_12_20_df_index['20天平均_1230-1255'].sum()/len(hl_12_20_df['20天平均_1230-1255'])



move_stop_loss =0.5
fix_stop_loss = 0.1
moneyness = 1
# C_itm_50 = 0
# C_itm_100 = -1
# P_itm_50 = 1
inintial_cash = 1000
global profit_total
profit_total = 0
CorP = 'C'
hedge_frequency = 10
stop_loss =0.4
stop_earn = 5
trade_time = 1
waiting_time = 60 

# record_df = pd.DataFrame(columns=['時間','BorS','履約價','CorP','價格','數量','手續費','損益','損益累計'])


    
def find_most_itm_option(future_price):
    itm_strike_price = future_price -future_price%50
    return itm_strike_price

def find_call_near(price):
    if price%50>25:
        strike_price = price +50 -price%50
    elif price%50<=25:
        strike_price = price -price%50
    return strike_price

def find_put_near(price):
    if price%50>25:
        strike_price = price+50 -price%50
    elif price%50<=25:
        strike_price = price -price%50
    return strike_price
    

def filter_merged_df(merged_df, **kwargs):
    """
    根據 kwargs 中的鍵和值來過濾 merged_df
    """
    condition = pd.Series([True] * len(merged_df))
    for key, value in kwargs.items():
        condition &= (merged_df[key] == value)
    return merged_df.loc[condition]

# 履約價格=call_strike, 買賣權別='C'

# merged_df[(merged_df['履約價格'] == call_strike) & (merged_df['買賣權別'] == 'C')][['時間', '選擇權_收盤價', 'iv']]

def process_row(index, row, end_date_df, result_queue):
    if index == 0 or index > 495:
        return
    # index=1
        
    
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
    n=trade_time
    waiting_time_count =waiting_time
    out_action=False
    
    option = Option()
    
    high_9 = 0
    low_9  = 5000000
    for future_index, future_row in month_future_price_df_raw.iterrows():
        # future_index = 195
        # future_row = month_future_price_df_raw.iloc[future_index]
        
        if (future_row['時間'].strftime("%Y-%m-%d") == end_date.strftime("%Y-%m-%d")) & ((future_row['時間'].hour >= 11) & (future_row['時間'].minute >= 30)) :
            # print(future_row['時間'])
            # print(future_index)
            
            if ((future_row['時間'].hour == 11) & (future_row['時間'].minute >= 30)) or ((future_row['時間'].hour == 12) & (future_row['時間'].minute <= 0)):
                
                if month_index[month_index['分鐘時間']==future_row['時間']]['最高價'].values[0] > high_9:
                    high_9 = month_index[month_index['分鐘時間']==future_row['時間']]['最高價'].values[0]
                    
                if month_index[month_index['分鐘時間']==future_row['時間']]['最低價'].values[0] < low_9:
                    low_9 = month_index[month_index['分鐘時間']==future_row['時間']]['最低價'].values[0]
                    
            

            
            # 買入點：當時間為9點，且沒有部位，且還有交易次數
            
            if out_action==True:
                waiting_time -=1
                
            if waiting_time ==0:
                out_action = False
                waiting_time =60
                

            
            
            
            if (((future_row['時間'].hour == 12) & (future_row['時間'].minute >0 ))and((future_row['時間'].hour == 12)& (future_row['時間'].minute <=30 ))) & (option.position_list['option']==[])& (n>0):
                # choose_strike = find_most_itm_option(future_price = future_row['收盤價'])+moneyness*50
                


                
                call_strike = find_call_near(low_9+hl_12_index)
                put_strike = find_put_near(high_9-hl_12_index)
                
                # call_strike = find_call_near(low_9)
                # put_strike = find_put_near(high_9)
                
                option_row_cal =  filter_merged_df(merged_df, 時間=future_row['時間'], 履約價格=call_strike, 買賣權別='C')
                option_row_cal_1 = filter_merged_df(merged_df, 時間=future_row['時間']+timedelta(minutes=1), 履約價格=call_strike, 買賣權別='C')
                option_row_put =  filter_merged_df(merged_df, 時間=future_row['時間'], 履約價格=put_strike, 買賣權別='P')
                option_row_put_1 = filter_merged_df(merged_df, 時間=future_row['時間']+timedelta(minutes=1), 履約價格=put_strike, 買賣權別='P')
                if (option_row_cal.empty) or (option_row_put.empty):
                    continue
                
                # call_price = option_row_cal['選擇權_收盤價'].values[0]
                # put_price = option_row_put['選擇權_收盤價'].values[0]
                
                # if ((option_row_cal['選擇權_收盤價'].values[0]>10) and (option_row_put['選擇權_收盤價'].values[0]>10) or  ((option_row_cal['選擇權_收盤價'].values[0]+option_row_put['選擇權_收盤價'].values[0])>20)):
                #     continue
                
                # while call_price>5:
                #     call_strike+=50
                #     option_row_cal =  filter_merged_df(merged_df, 時間=future_row['時間'], 履約價格=call_strike, 買賣權別='C')
                #     if option_row_cal.empty:
                #         continue
                #     else:
                #         call_price = option_row_cal['選擇權_收盤價'].values[0]
                    
                # while put_price>5:
                #     put_strike-=50
                #     option_row_put =  filter_merged_df(merged_df, 時間=future_row['時間'], 履約價格=put_strike, 買賣權別='P')
                #     if option_row_put.empty:
                #         continue
                #     else:
                #         put_price = option_row_put['選擇權_收盤價'].values[0]


                # price = option_row['選擇權_收盤價'].values[0]
                # delta = option_row['delta'].values[0]
                # hege_delta = hedge_df[(hedge_df['時間']==future_row['時間'].strftime('%H:%M:%S')) & (hedge_df['價平檔位']==-moneyness*50)& (hedge_df['買賣權別']==CorP)]['delta'].values[0]
                # future_delta  = -hege_delta+delta
                # if delta<0.5 and option.position_list['option']==[]:
                    # out_action = True
                    
                if future_row['收盤價']<(high_9+hl_12)/2:
                    option.option_trade(future_row['時間'],'B','C',call_strike,option_row_cal['選擇權_收盤價'].values[0],1,0,position_action='open',record_dict={'iv':option_row_cal['iv'].values[0]})
                    option.option_trade(future_row['時間']+timedelta(minutes=1),'B','P',put_strike,option_row_put_1['選擇權_收盤價'].values[0],1,0,position_action='open',record_dict={'iv':option_row_put['iv'].values[0]})
                
                else:
                    option.option_trade(future_row['時間'],'B','P',put_strike,option_row_put_1['選擇權_收盤價'].values[0],1,0,position_action='open',record_dict={'iv':option_row_put['iv'].values[0]})
                    option.option_trade(future_row['時間']+timedelta(minutes=1),'B','C',call_strike,option_row_cal_1['選擇權_收盤價'].values[0],1,0,position_action='open',record_dict={'iv':option_row_cal['iv'].values[0]})
                    
                
                    
                n-=1

                    
                
            
            
            
            if (future_row['時間'].hour == 13) & (future_row['時間'].minute >= 30):

                if option.position_list['option']!=[]:
                    
                    positions_to_close = option.position_list['option'][:]

                    for position in positions_to_close:
                        if position['BorS'] == 'B':
                            option.option_trade(future_row['時間'], 'S', position['CorP'], position['履約價'], position['現價'], 1, 0, position_action='close')
                        else:
                            option.option_trade(future_row['時間'], 'B', position['CorP'], position['履約價'], position['現價'], 1, 0, position_action='close')

            # 有選擇權部位
            if option.position_list['option']!=[]:
            
                # 更新position_list
                for position in option.position_list['option']:
                    # position =  option.position_list['option'][0]
                    
                    option_row =  filter_merged_df(merged_df, 時間=future_row['時間'], 履約價格=position['履約價'], 買賣權別=position['CorP'])
                    if option_row.empty:
                        # 13:29就算沒有也要平倉
                        
                            continue
        
                    else:
                        
                        position['現價'] = option_row['選擇權_收盤價'].values[0]
                        
                        
                    if position['價格']*stop_earn<=position['現價']:
                        if position['BorS'] == 'B':
                            option.option_trade(future_row['時間'], 'S', position['CorP'], position['履約價'], position['現價'], 1, 0, position_action='close')
                        else:
                            option.option_trade(future_row['時間'], 'B', position['CorP'], position['履約價'], position['現價'], 1, 0, position_action='close')
                    # position['現價'] = option_row['選擇權_收盤價'].values[0]
                    

            #         # # #TODO 0.5就出場
            #         # if option_row['delta'].values[0]>0.5:
            #         #     option.option_trade(future_row['時間'],'B',CorP,position['履約價'],position['現價'],1,0,position_action='close',record_dict={'delta':delta})
                    
    if n==0:
        record_dict = {
            '日期':start_date.strftime('%Y-%m-%d'),
            '加權指數':month_index.sort_values(by='分鐘時間'),
            '期貨':month_future_price_df_raw,
            '選擇權紀錄':option.record_list['option'],
            '期貨紀錄':option.record_list['future'],
            'call_價格': merged_df[(merged_df['履約價格'] == call_strike) & (merged_df['買賣權別'] == 'C')][['時間', '選擇權_收盤價', 'iv']],
            'put_價格': merged_df[(merged_df['履約價格'] == put_strike) & (merged_df['買賣權別'] == 'P')][['時間', '選擇權_收盤價', 'iv']]
        }
        result_queue.put((index, record_dict))     
    if n!=0:
        result_queue.put((index, None))
    # record_list.append(record_dict)
    
    
#0.8 
# for stop_loss in [0,0.1,0.2,0.3,0.4,0.5,0.6,0.7]:
#     # stop_loss = 0
#     for stop_earn in [2,3,4,5,6,7,8,9,10]:
#         # stop_earn=100
#         for average in [0]:
#         # average = None
#             # 使用多线程并行处理
            
            
stop_loss = 0.4
stop_earn = 100
average = None   
result_queue = queue.Queue()
with ThreadPoolExecutor(max_workers=8) as executor:
    futures = [executor.submit(process_row, index, row, end_date_df, result_queue) for index, row in end_date_df.iterrows()]
    for future in futures:
        future.result()
        
record  = Record(result_queue,len(end_date_df))
record.output_excel('雙買_12hl_20average')

