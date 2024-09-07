import os
from datetime import datetime
from functools import reduce

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pymongo

from util.BS_util import BS_formula


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
    month_future_price_df.reset_index(drop=True, inplace=True)
    
    
    return month_future_price_df

df_end_date_info = pd.read_excel('結算日日期和種類 (1).xlsx')

# 將月份和日期轉換為兩位數格式
df_end_date_info['年'] = df_end_date_info['年'].astype(str)
df_end_date_info['月'] = df_end_date_info['月'].astype(str).str.zfill(2)
df_end_date_info['日'] = df_end_date_info['日'].astype(str).str.zfill(2)

df_end_date_info['結算日'] = pd.to_datetime(df_end_date_info['年'] + df_end_date_info['月'] + df_end_date_info['日'], format='%Y%m%d')

# Set the time part to 13:30:00
df_end_date_info['結算日'] = df_end_date_info['結算日'].apply(lambda x: x.replace(hour=13, minute=30, second=0))

def get_label(x):
    match x:
        case 'TXO':
            return '月'
        case 'TX1' | 'TX2' | 'TX4' | 'TX5':
            return '週'
        case _:
            return '其他'

df_end_date_info['周月註記'] = df_end_date_info['種類'].apply(get_label)

df_end_date_info['結算日_日期'] = pd.to_datetime(df_end_date_info['結算日']).dt.date


# Define the date range
start_date_str = '2015-1-01'
end_date_str = '2024-8-7'

# Parse the date strings into datetime objects
start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
end_date = datetime.strptime(end_date_str, '%Y-%m-%d')


month_future_price_df_raw = read_future_price(start_date,end_date) 
month_future_price_df_raw['日期'] =pd.to_datetime(month_future_price_df_raw['時間']).dt.date

    


merged_df = pd.merge(month_future_price_df_raw, df_end_date_info[['結算日_日期', '周月註記']], left_on='日期', right_on='結算日_日期', how='left')

merged_df.to_pickle('2015~2024month_future_price_df.pkl')
