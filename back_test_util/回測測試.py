import os
from datetime import datetime, timedelta

from functools import reduce

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pymongo



def read_data(start_date,end_date):
    data_dict = {}
    for key in ['C','P']:
        # 連接到MongoDB
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client["backtest"]
        collection = db[f"{key}_price"]


        # 查詢符合條件的記錄
        query = {'時間': {'$gte': start_date, '$lte':end_date}}
        cursor = collection.find(query)


        mongo_docs = list(cursor)

        df = pd.DataFrame(mongo_docs)
        df['時間'] = pd.to_datetime(df['時間'])
        
        data_dict[f'{key}_price'] = df
        
    return data_dict



def read_future_price(start_date,end_date):
    # # Define the date range
    # start_date_str = '2022-12-01'
    # end_date_str = '2022-12-31'

    # # Parse the date strings into datetime objects
    # start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    # end_date = datetime.strptime(end_date_str, '%Y-%m-%d')


    # 連接到MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["option_price"]
    collection = db["future"]


    # 查詢符合條件的記錄
    query = {'時間': {'$gte': start_date, '$lte':end_date}, '商品代號':"TX"}
    cursor = collection.find(query)


    mongo_docs = list(cursor)

    df = pd.DataFrame(mongo_docs)
    
    
    # 確保 '到期月份(週別)' 列是數字類型
    df['到期月份(週別)'] = pd.to_numeric(df['到期月份(週別)'], errors='coerce')
    df['時間'] = pd.to_datetime(df['時間'])

    
    df['成交日期'] = df['時間'].dt.date

    # 找到每個成交日期中到期月份(週別)最小的值
    min_expiry_per_date = df.groupby('成交日期')['到期月份(週別)'].transform('min')

    # 過濾原始 DataFrame，保留到期月份(週別)等於最小值的所有行
    month_future_price_df = df[df['到期月份(週別)'] == min_expiry_per_date]
    
    month_future_price_df.sort_values(by='時間', inplace=True)
    
    
    return month_future_price_df

def read_end_date():
    df_end_date_info = pd.read_csv('結算日期.csv', encoding='utf-8',index_col=0)

    # 將月份和日期轉換為兩位數格式
    df_end_date_info['年'] = df_end_date_info['年'].astype(str)
    df_end_date_info['月'] = df_end_date_info['月'].astype(str).str.zfill(2)
    df_end_date_info['日'] = df_end_date_info['日'].astype(str).str.zfill(2)

    df_end_date_info['結算日'] = pd.to_datetime(df_end_date_info['年'] + df_end_date_info['月'] + df_end_date_info['日'], format='%Y%m%d')

    # Set the time part to 13:30:00
    df_end_date_info['結算日'] = df_end_date_info['結算日'].apply(lambda x: x.replace(hour=13, minute=30, second=0))


    return df_end_date_info

# 計算 '時間' 和 '結算日' 之間的差異
merged_df['差異'] = merged_df['結算日'] - merged_df['時間']

# 提取差異中的整天數
merged_df['整天數'] = merged_df['差異'].dt.days

# 加入VIX指數
vix_index = pd.read_excel('vix.xlsx')
vix_index['時間'] = pd.to_datetime(vix_index['時間'])
vix_index['時間代碼'] = vix_index['時間'].dt.strftime('%Y%m%d')
vix_index['vix'] = vix_index['開盤價']/100
merged_df = pd.merge(merged_df, vix_index[['時間代碼','vix']], left_on='成交日期_option', right_on='時間代碼', how='left')


# 加入公債
bond_index = pd.read_excel('公債.xlsx')
bond_index['時間'] = pd.to_datetime(bond_index['時間'])
bond_index['時間代碼'] = bond_index['時間'].dt.strftime('%Y%m%d')
bond_index['公債'] = bond_index['開盤價']/100
merged_df = pd.merge(merged_df, bond_index[['時間代碼','公債']], left_on='成交日期_option', right_on='時間代碼', how='left')

# 計算T
merged_df['T'] = merged_df['差異'].dt.total_seconds() / (252* 24 * 60*60 )


# 參考S
merged_df['參考s'] = np.where(merged_df['差異'] < pd.Timedelta('0 days 00:30:00'), merged_df['指數_收盤價'], merged_df['期貨_收盤價'])

# # 理論價格
# merged_df['bs_price'] = merged_df.apply(get_bs_price, axis=1)


# 計算所有指標並輸出到 DataFrame 中
bs_formula = BS_formula(
    merged_df['參考s'],
    merged_df['履約價格'],
    merged_df['公債'],
    merged_df['vix'],
    merged_df['T'],
)

merged_df['bs_price'] = np.where(merged_df['買賣權別'] == 'C', bs_formula.BS_price()[0], bs_formula.BS_price()[1])

# merged_df['delta'] = bs_formula.BS_delta()[0].round(7)
merged_df['delta'] = np.where(merged_df['買賣權別'] == 'C', bs_formula.BS_delta()[0].round(7), bs_formula.BS_delta()[1].round(7))

# merged_df['gamma'] = bs_formula.BS_gamma()[0].round(7)
merged_df['gamma'] = np.where(merged_df['買賣權別'] == 'C', bs_formula.BS_gamma()[0].round(7), bs_formula.BS_gamma()[1].round(7))


merged_df['vega'] = np.where(merged_df['買賣權別'] == 'C', bs_formula.BS_vega()[0].round(7), bs_formula.BS_vega()[1].round(7))


# merged_df['rho'] = bs_formula.BS_rho()[0]
merged_df['rho'] = np.where(merged_df['買賣權別'] == 'C', bs_formula.BS_rho()[0].round(7), bs_formula.BS_rho()[1].round(7))


# bs_theta = bs_formula.BS_theta()
merged_df['theta'] = np.where(merged_df['買賣權別'] == 'C', bs_formula.BS_theta()[0],bs_formula.BS_theta()[1])


end_date_df = read_end_date()

move_stop_loss =0.1
moneyness = 0
# C_itm_1 = 0
# C_itm_2 = 1
# P_itm_1 = -1
inintial_cash = 1000
global profit_total
profit_total = 0
CorP = 'C'

position_list = []

record_list = []
record_df = pd.DataFrame(columns=['時間','BorS','履約價','CorP','價格','數量','手續費','損益','損益累計'])

def buy_call(timestamp,strike_price,price,quantity,fee,profit_total):
    profit = -price*quantity-fee
    profit_total += profit
    record_list.append({'時間':timestamp,'BorS':'B','履約價':strike_price,'CorP':'C','價格':price,'數量':quantity,'手續費':fee,'損益':profit,'損益累計':profit_total})
    

    position_list.append({'時間':timestamp,'BorS':'B','履約價':strike_price,'CorP':'C','價格':price,'數量':quantity})
    print({'時間':timestamp,'BorS':'B','履約價':strike_price,'CorP':'C','價格':price,'數量':quantity,'手續費':fee,'損益':profit,'損益累計':profit_total})
    return price,profit_total

def sell_call(timestamp,strike_price,price,quantity,fee,profit_total):
    profit = price*quantity-fee
    profit_total += profit

    record_list.append({'時間':timestamp,'BorS':'S','履約價':strike_price,'CorP':'C','價格':price,'數量':quantity,'手續費':fee,'損益':profit,'損益累計':profit_total})

    position_list.pop()
    print({'時間':timestamp,'BorS':'S','履約價':strike_price,'CorP':'C','價格':price,'數量':quantity,'手續費':fee,'損益':profit,'損益累計':profit_total})
    return price,profit_total
    
def find_most_itm_option(future_price):
    itm_strike_price = future_price -future_price%50
    return itm_strike_price
    
for index, row in end_date_df.iterrows():
    # index = 2
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
        

        print(start_date,end_date)
        data_dict = read_data(start_date,end_date)
        data_dict['future_price'] = read_future_price(start_date,end_date)
        
        data_dict['vix']
        
        option_df = data_dict[f'{CorP}_price']

        for future_index, future_row in data_dict['future_price'].iterrows():
            # future_index =524
            # future_row = data_dict['future_price'].iloc[future_index]
            print(future_row['時間'])
            if (future_row['時間'].strftime("%Y-%m-%d") == end_date.strftime("%Y-%m-%d")) & (future_row['時間'].hour >= 9):
                # future_row['收盤價']
                choose_strike = str(int(find_most_itm_option(future_price = future_row['收盤價'])+moneyness*50))
                try:
                    price = option_df[option_df['時間']==future_row['時間']][choose_strike].values[0]
                
                except:
                    choose_strike = str(int(find_most_itm_option(future_price = future_row['收盤價'])+(moneyness+1)*50))
                    price = option_df[option_df['時間']==future_row['時間']][choose_strike].values[0]
                
                if (future_row['時間'].hour == 9) & (future_row['時間'].minute == 0):
                    choose_strike = str(int(find_most_itm_option(future_price = future_row['收盤價'])+moneyness*50))
                    
                    
                    
                    
                    price, profit_total = buy_call(future_row['時間'],choose_strike,price,1,0,profit_total)
                
                if position_list!=[]:
                    if  price <= (1-move_stop_loss)*position_list[0]['價格']:
                        price, profit_total = sell_call(future_row['時間'],choose_strike,price,1,0,profit_total)
                    
                    if (future_row['時間'].hour == 13) & (future_row['時間'].minute >= 25):
                        price,profit_total = sell_call(future_row['時間'],choose_strike,price,1,0,profit_total)
                        
                        
                    
                