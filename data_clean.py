import os
import shutil
import zipfile

import pandas as pd


# 解壓縮檔案
def unzip_files(source_dir, dest_dir):
    for item in os.listdir(source_dir): # loop through items in dir
        if item.endswith('.zip'): # check for ".zip" extension
            file_name = source_dir + "/" + item # get full path of files
            zip_ref = zipfile.ZipFile(file_name) # create zipfile object
            zip_ref.extractall(dest_dir) # extract file to dir
            zip_ref.close() # close file
            # os.remove(file_name) # delete zipped file
        else:
            continue


# 解壓縮所有的資料並移動到./data
# 選定zip資料夾
source_dir = './選擇權日內成交檔資料11002-11004'
dest_dir = './data/raw_data'
unzip_files(source_dir, dest_dir)


import glob

#%%


# 找到所有的txt檔案
txt_files = glob.glob('./data/raw_data/*.txt')

# 欄位名稱列表
column_names = ['MTF_DATE', 'MTF_PROD_ID', 'MTF_PROD_ID1', 'MTF_BS_CODE1', 'MTF_M_PRICE1', 'MTF_M_QNTY1', 'MTF_PROD_ID2', 'MTF_BS_CODE2', 'MTF_M_PRICE2', 'MTF_M_QNTY2', 'MTF_BS_CODE', 'MTF_PRICE', 'MTF_QNTY', 'MTF_OC_CODE', 'MTF_M_INST', 'MTF_ORIG_TIME', 'MTF_SC_CODE', 'FCM_NO+ORDER_NO+', 'OQ_CODE']

# 讀取所有的txt檔案
dataframes = []
for txt_file in txt_files:
    df = pd.read_csv(txt_file, delimiter = "\t", names=column_names) # 使用names參數來設定欄位名稱
    dataframes.append(df)

# df = pd.read_csv('data/OWMTF_20210201.txt', delimiter = "\t", names=column_names) # 使用names參數來設定欄位名稱



# 將所有的dataframe合併成一個
all_data_df = pd.concat(dataframes, ignore_index=True)

all_data_df.to_csv('all_data.csv', index=False, encoding='utf-8-sig')


#%%
all_data_df = pd.read_csv('all_data.csv', encoding='utf-8-sig')

# 只留下MTD_PROD_ID含有TX的台指期選擇權
all_data_df = all_data_df[all_data_df['MTF_PROD_ID'].str.contains('TXO')]

# 只留下MTD_PROD_ID長度為10的台指期選擇權為單式商品
all_data_df = all_data_df[all_data_df['MTF_PROD_ID'].str.len() == 9]

# 新增履約價格欄位
all_data_df['strike_price'] = all_data_df['MTF_PROD_ID'].str.slice(3, 8)

# 新增履約月份欄位
all_data_df['month_alpha'] = all_data_df['MTF_PROD_ID'].str.slice(8, 9)

# 月份映射Ａ~L(112月)買權,M~X(112月)賣權
alpha_to_month = {
    'A': 1, 'B': 2, 'C': 3, 'D': 4, 'E': 5, 'F': 6,
    'G': 7, 'H': 8, 'I': 9, 'J': 10, 'K': 11, 'L': 12,
    'M': 1, 'N': 2, 'O': 3, 'P': 4, 'Q': 5, 'R': 6,
    'S': 7, 'T': 8, 'U': 9, 'V': 10, 'W': 11, 'X': 12
}

# 轉換month_alpha
all_data_df['month'] = all_data_df['month_alpha'].map(alpha_to_month)

# 轉換month為字串
all_data_df['month'] = all_data_df['month'].astype(int).astype(str)


# MTF_PROD_ID分群
prod_df = all_data_df.groupby('MTF_PROD_ID')


import re

# 輸出每一組的數據到一個csv檔案
for name, group in prod_df:
    cleaned_name = re.sub('[\W_]+', '', name)
    group.to_csv(f'./data/group/{cleaned_name}.csv', index=False, encoding='utf-8-sig')



