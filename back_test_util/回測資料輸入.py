# 書整理成回測資料

import os
from datetime import datetime, timedelta

from functools import reduce

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pymongo

from util.BS_util import BS_formula

def read_index(start_date,end_date):
    # # Define the date range
    # start_date_str = '2022-12-01'
    # end_date_str = '2022-12-31'

    # # Parse the date strings into datetime objects
    # start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    # end_date = datetime.strptime(end_date_str, '%Y-%m-%d')


    # 連接到MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["option_price"]
    collection = db["index"]


    # 查詢符合條件的記錄
    query = {'分鐘時間': {'$gte': start_date, '$lte':end_date}}
    cursor = collection.find(query)


    mongo_docs = list(cursor)

    df = pd.DataFrame(mongo_docs)

    return df

def read_option_price(start_date,end_date,week_kind):
    # # Define the date range
    # start_date_str = '2022-12-01'
    # end_date_str = '2022-12-31'

    # # Parse the date strings into datetime objects
    # start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    # end_date = datetime.strptime(end_date_str, '%Y-%m-%d')


    # 連接到MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["option_price"]
    collection = db["option"]


    # 查詢符合條件的記錄
    query = {'時間': {'$gte': start_date, '$lte':end_date}, '商品代號':"TXO","到期月份(週別)":week_kind}
    cursor = collection.find(query)


    mongo_docs = list(cursor)

    df = pd.DataFrame(mongo_docs)
    


    return df

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

end_date_df = read_end_date()
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["backtest"]


for index, row in end_date_df.iterrows():
    # index = 388
    if index == 0:
        continue
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

        month_option_price_df = read_option_price(start_date,end_date,week_kind) 

        month_future_price_df = read_future_price(start_date,end_date) 

        # month_future_price_df.sort_values(by='時間', inplace=True)

        month_index = read_index(start_date,end_date) 
        month_index.sort_values(by='分鐘時間', inplace=True)

        # end_date_df = read_end_date()

        merged_option_df = pd.merge(month_option_price_df, end_date_df[['到期月份(週別)', '結算日']] , on='到期月份(週別)', how='left')
        # 計算 '時間' 和 '結算日' 之間的差異
        merged_option_df['差異'] = merged_option_df['結算日'] - merged_option_df['時間']
        
        


        # 整合成c or p 資料
        merge_df = pd.DataFrame()

        merge_df = pd.merge(month_future_price_df[['時間','收盤價']], month_index[['分鐘時間', '收盤價']], left_on='時間',right_on='分鐘時間', how='left', suffixes=('_期貨', '_加權指數'))

        merge_df.sort_values(by='時間', inplace=True)
        merge_df.drop(columns=['分鐘時間'], inplace=True)

        cp_df_dict = {
            "C":merge_df.copy(),
            "P":merge_df.copy()
        }

        merged_option_df['履約價格'] = merged_option_df['履約價格'].astype(int)
        for key in cp_df_dict:
            # key= 'C'
            for strike_price in sorted(merged_option_df['履約價格'].unique()):
                # strike_price = '13700'
                # print(strike_price)
                option_df = merged_option_df[(merged_option_df['履約價格']==strike_price)
                                                                    & (merged_option_df['買賣權別']==key)
                                                                    ].rename(columns={'收盤價':'收盤價_選擇權'})
                cp_df_dict[key][str(strike_price)] = pd.merge(cp_df_dict[key],option_df[['時間','收盤價_選擇權']], on='時間', how='left')['收盤價_選擇權'].astype(float)
                cp_df_dict[key][str(strike_price)] = cp_df_dict[key][str(strike_price)].ffill().bfill()
        
                # Infer objects to ensure correct data types
                # cp_df_dict[key][strike_price] = cp_df_dict[key][strike_price].infer_objects(copy=False)
                # try_df =cp_df_dict[key]

            records = cp_df_dict[key].to_dict('records')
    
            # Insert the records into the collection
            db[f'{key}_price'].insert_many(records)
            
        

