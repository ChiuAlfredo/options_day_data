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
    query = {'時間': {'$gte': start_date, '$lte':end_date},'到期月份(週別)': week_kind, '商品代號':"TXO"}
    cursor = collection.find(query)




    mongo_docs = list(cursor)

    df = pd.DataFrame(mongo_docs)
    
    df.sort_values(by='時間', inplace=True)
    


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
    month_future_price_df.reset_index(drop=True, inplace=True)
    
    
    return month_future_price_df



# def read_data(start_date,end_date):
#     data_dict = {}
#     for key in ['C','P']:
#         # 連接到MongoDB
#         client = pymongo.MongoClient("mongodb://localhost:27017/")
#         db = client["backtest"]
#         collection = db[f"{key}_price"]


#         # 查詢符合條件的記錄
#         query = {'時間': {'$gte': start_date, '$lte':end_date}}
#         cursor = collection.find(query)


#         mongo_docs = list(cursor)

#         df = pd.DataFrame(mongo_docs)
#         df['時間'] = pd.to_datetime(df['時間'])
        
#         data_dict[f'{key}_price'] = df
        
#     return data_dict





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

def buil_Data(month_option_price_df,month_future_price_df_raw,month_index,end_date_df):
    


    # 確保 '到期月份(週別)' 列是數字類型
    month_future_price_df_raw['到期月份(週別)'] = pd.to_numeric(month_future_price_df_raw['到期月份(週別)'], errors='coerce')

    # 找到每個成交日期中到期月份(週別)最小的值
    min_expiry_per_date = month_future_price_df_raw.groupby('成交日期')['到期月份(週別)'].transform('min')

    # 過濾原始 DataFrame，保留到期月份(週別)等於最小值的所有行
    month_future_price_df = month_future_price_df_raw[month_future_price_df_raw['到期月份(週別)'] == min_expiry_per_date]



    def map_code(row):
        if 'W1' in row['到期月份(週別)']  :
            return 'TX1'
        elif 'W2' in row['到期月份(週別)']  :
            return 'TX2'
        elif 'W4' in row['到期月份(週別)'] :
            return 'TX4'
        elif 'W5' in row['到期月份(週別)'] :
            return 'TX5'
        else:
            return row['商品代號']

    # Apply the function to update the '商品代碼' column
    month_option_price_df['商品代號'] = month_option_price_df.apply(map_code, axis=1)

    month_option_price_df = month_option_price_df.rename(columns={
        '開盤價': '選擇權_開盤價',
        '收盤價': '選擇權_收盤價',
        '最高價': '選擇權_最高價',
        '最低價': '選擇權_最低價',
        '成交量': '選擇權_成交量'
    })

    month_future_price_df = month_future_price_df.rename(columns={
        '開盤價': '期貨_開盤價',
        '收盤價': '期貨_收盤價',
        '最高價': '期貨_最高價',
        '最低價': '期貨_最低價',
        '成交量': '期貨_成交量'
    })

    month_index = month_index.rename(columns={
        '開盤價': '指數_開盤價',
        '收盤價': '指數_收盤價',
        '最高價': '指數_最高價',
        '最低價': '指數_最低價',
    })

    # 將指定列轉換為 float 類型
    # 定義需要轉換的列
    columns_to_convert_option = ['選擇權_開盤價', '選擇權_收盤價', '選擇權_最高價', '選擇權_最低價', '選擇權_成交量','履約價格']
    columns_to_convert_future = ['期貨_開盤價', '期貨_收盤價', '期貨_最高價', '期貨_最低價', '期貨_成交量']
    columns_to_convert_index = ['指數_開盤價', '指數_收盤價', '指數_最高價', '指數_最低價']

    # 去除逗號並轉換為 float 類型
    month_option_price_df[columns_to_convert_option] = month_option_price_df[columns_to_convert_option].replace(',', '', regex=True).astype(float)
    month_future_price_df[columns_to_convert_future] = month_future_price_df[columns_to_convert_future].replace(',', '', regex=True).astype(float)
    month_index[columns_to_convert_index] = month_index[columns_to_convert_index]



    merged_df = pd.merge(month_option_price_df, month_future_price_df,how='left', on='時間', suffixes=('_option', '_future'))


    merged_df = pd.merge(merged_df, month_index, left_on='時間',how='left',right_on='分鐘時間', suffixes=('', '_index'))

    merged_df = pd.merge(merged_df, end_date_df[['到期月份(週別)', '結算日']] , left_on='到期月份(週別)_option', right_on='到期月份(週別)', how='left')


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


    # 先篩選 T 不為 0 的數據
    filtered_df = merged_df[merged_df['T'] != 0]

    # 計算所有指標並輸出到 DataFrame 中
    bs_formula = BS_formula(
        filtered_df['參考s'],
        filtered_df['履約價格'],
        filtered_df['公債'],
        filtered_df['vix'],
        filtered_df['T'],
        filtered_df['選擇權_收盤價']
    )

    filtered_df['bs_price'] = np.where(filtered_df['買賣權別'] == 'C', bs_formula.BS_price()[0], bs_formula.BS_price()[1])

    filtered_df['delta'] = np.where(filtered_df['買賣權別'] == 'C', bs_formula.BS_delta()[0].round(7), bs_formula.BS_delta()[1].round(7))

    filtered_df['gamma'] = np.where(filtered_df['買賣權別'] == 'C', bs_formula.BS_gamma()[0].round(7), bs_formula.BS_gamma()[1].round(7))

    filtered_df['vega'] = np.where(filtered_df['買賣權別'] == 'C', bs_formula.BS_vega()[0].round(7), bs_formula.BS_vega()[1].round(7))

    filtered_df['rho'] = np.where(filtered_df['買賣權別'] == 'C', bs_formula.BS_rho()[0].round(7), bs_formula.BS_rho()[1].round(7))

    filtered_df['theta'] = np.where(filtered_df['買賣權別'] == 'C', bs_formula.BS_theta()[0], bs_formula.BS_theta()[1])

    filtered_df['iv'] = np.where(filtered_df['買賣權別'] == 'C', bs_formula.iv()[0].round(4), bs_formula.iv()[1].round(4))

    # 將計算結果合併回原始 DataFrame
    merged_df = merged_df.assign(
        bs_price=np.nan,
        delta=np.nan,
        gamma=np.nan,
        vega=np.nan,
        rho=np.nan,
        theta=np.nan,
        iv=np.nan
    )

    # 將計算結果合併回原始 DataFrame
    merged_df.update(filtered_df)
    return merged_df

  # cp_df_dict = {
        #     "C_price":month_future_price_df_raw['時間'].to_frame(),
        #     "P_price":month_future_price_df_raw['時間'].to_frame(),
        #     # 'Vix':merged_df['時間'].to_frame(),
        #     # "C_delta":merged_df['時間'].to_frame(),
        #     # "C_gamma":merged_df['時間'].to_frame(),
        #     # "C_vega":merged_df['時間'].to_frame(),
        #     # "C_rho":merged_df['時間'].to_frame(),
        #     # "C_theta":merged_df['時間'].to_frame(),
        #     # "C_bs_price":merged_df['時間'].to_frame(),
        #     # "P_delta":merged_df['時間'].to_frame(),
        #     # "P_gamma":merged_df['時間'].to_frame(),
        #     # "P_vega":merged_df['時間'].to_frame(),
        #     # "P_rho":merged_df['時間'].to_frame(),
        #     # "P_theta":merged_df['時間'].to_frame(),
        #     # "P_bs_price":merged_df['時間'].to_frame(),
        #     # "C_iv":month_future_price_df_raw['時間'].to_frame(),
        #     # "P_iv":month_future_price_df_raw['時間'].to_frame(),
        #     # "T":month_future_price_df_raw['時間'].to_frame(),
        #     # "參考s":month_future_price_df_raw['時間'].to_frame(),
        #     # "r":month_future_price_df_raw['時間'].to_frame(),
        #     # "公債":month_future_price_df_raw['時間'].to_frame(),
        # }

        # merged_df['履約價格'] = merged_df['履約價格'].astype(int)
        # for key in ['C','P']:
        #     # key= 'C'
        #     for strike_price in sorted(merged_df['履約價格'].unique()):
        #         # strike_price = 23000
        #         # print(strike_price)
        #         option_df = merged_df[(merged_df['履約價格']==strike_price)
        #                                                             & (merged_df['買賣權別']==key)
        #                                                             ]
        #         cp_df_dict[f'{key}_price'][str(strike_price)] = pd.merge(cp_df_dict[f'{key}_price'],option_df[['時間','選擇權_收盤價']], on='時間', how='left')['選擇權_收盤價'].ffill().bfill()
                
                # cp_df_dict[f'{key}_iv'][str(strike_price)] = pd.merge(cp_df_dict[f'{key}_iv'],option_df[['時間','iv']], on='時間', how='left')['iv']


                # cp_df_dict[f'{key}_price'][str(strike_price)] = cp_df_dict[f'{key}_price'][str(strike_price)].ffill().bfill()
                # cp_df_dict[f'{key}_price'][str(strike_price)] = pd.concat([cp_df_dict[f'{key}_price'], option_df[['時間', '選擇權_收盤價']]], axis=0).set_index('時間')['選擇權_收盤價']
                # cp_df_dict[f'{key}_price'].rename(columns={'選擇權_收盤價':str(strike_price)}, inplace=True)
                
               
                # try_df = cp_df_dict[f'{key}_price']
                
                # cp_df_dict[f'{key}_delta'][str(strike_price)] = pd.merge(cp_df_dict[f'{key}_delta'],option_df[['時間','delta']], on='時間', how='left')['delta'].astype(float)
                # # cp_df_dict[f'{key}_delta'][str(strike_price)] = cp_df_dict[f'{key}_delta'][str(strike_price)].ffill().bfill()
                # cp_df_dict[f'{key}_gamma'][str(strike_price)] = pd.merge(cp_df_dict[f'{key}_gamma'],option_df[['時間','gamma']], on='時間', how='left')['gamma'].astype(float)
                
                # cp_df_dict[f'{key}_vega'][str(strike_price)] = pd.merge(cp_df_dict[f'{key}_vega'],option_df[['時間','vega']], on='時間', how='left')['vega'].astype(float)
                
                # cp_df_dict[f'{key}_rho'][str(strike_price)] = pd.merge(cp_df_dict[f'{key}_rho'],option_df[['時間','rho']], on='時間', how='left')['rho'].astype(float)
                
                # cp_df_dict[f'{key}_theta'][str(strike_price)] = pd.merge(cp_df_dict[f'{key}_theta'],option_df[['時間','theta']], on='時間', how='left')['theta'].astype(float)
                
                # cp_df_dict[f'{key}_bs_price'][str(strike_price)] = pd.merge(cp_df_dict[f'{key}_bs_price'],option_df[['時間','bs_price']], on='時間', how='left')['bs_price'].astype(float)

            
            
            

        
        # option_df = cp_df_dict[f'{CorP}_price']
        # iv_df = cp_df_dict[f'{CorP}_iv']
        
        

        
        # # 用iv補資料
        # iv_no_time_df = iv_df.drop(columns='時間')
        # def fill_na_with_interpolation(series):
        #     """
        #     將 series 中的空值使用左右相鄰值的平均值補上
        #     """
        #     # series = iv_no_time_df.iloc[0]
        #     for _ in range(2):  # 執行兩次
        #         series = series.fillna((series.shift(1) + series.shift(-1)) / 2)
        #     return series
        #         # 假設 iv_no_time_df 是一個 DataFrame
        # iv_no_time_df_new = iv_no_time_df.apply(fill_na_with_interpolation)
                  