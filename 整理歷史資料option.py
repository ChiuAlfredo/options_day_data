
import os
import shutil
import zipfile
from datetime import datetime

import pandas as pd
from pymongo import MongoClient
import threading


def to_minutes(data_list):
    df = pd.DataFrame(data_list)
    df['TimeStamp'] = pd.to_datetime(df['成交日期'] + df['成交時間'], format='%Y%m%d%H%M%S')
    df['時間'] = df['TimeStamp'].dt.ceil('T')
    df = df.sort_values(by='TimeStamp')

    # group_key = ('20240102', 'TXO', '15900', '202401', 'P')
    grouped_df = df.groupby(['成交日期', '商品代號', '履約價格', '到期月份(週別)', '買賣權別','時間'])
    try:
        result = grouped_df.agg(
            開盤價=('成交價格', 'first'),
            收盤價=('成交價格', 'last'),
            最高價=('成交價格', 'max'),
            最低價=('成交價格', 'min'),
            成交量=('成交數量(B or S)', lambda x: x.astype(int).sum() / 2)
                ).reset_index()
    except:
        result = grouped_df.agg(
            開盤價=('成交價格', 'first'),
            收盤價=('成交價格', 'last'),
            最高價=('成交價格', 'max'),
            最低價=('成交價格', 'min'),
            成交量=('成交數量(BorS)', lambda x: x.astype(int).sum() / 2)
                ).reset_index()


    result_list = result.to_dict(orient='records')

    return result_list


    

def read_rpt_insert(file):
    with open(os.path.join(folder_path, folder, file), 'r') as f:
        lines = f.readlines()

    lines

    # 提取第一行作為欄位名稱
    header = [h.strip().replace(' ','') for h in lines[0].strip().split(',')]

    data_list = []
    if any('--' in line for line in lines[1:2]):
        for line in lines[2:]:
            values = [value.strip().replace(' ','') for value in line.strip().split(',')]
            data_dict = {header[i]: values[i] for i in range(len(header))}
            data_list.append(data_dict)

    else:
        for line in lines[1:]:
            values = [value.strip().replace(' ','') for value in line.strip().split(',')]
            data_dict = {header[i]: values[i] for i in range(len(header))}
            data_list.append(data_dict)

    result_list = to_minutes(data_list)

    # 將數據插入到 MongoDB
    price_collection.insert_many(result_list)
    print(f'{file} has been inserted into MongoDB')


def unzip_file(source_dir, target_dir):
    # source_dir = os.path.join(folder_path, folder, file)
    # target_dir = os.path.join(folder_path, folder)
    if source_dir.endswith('.zip'):  # check for ".zip" extension
        zip_ref = zipfile.ZipFile(source_dir)  # create zipfile object
        zip_ref.extractall(target_dir)  # extract file to dir
        zip_ref.close()  # close file


# 設置MongoDB連接
client = MongoClient('mongodb://localhost:27017/')
db = client['option_price']
price_collection = db['option']

# 指定資料夾路徑
folder_path = './data/TaifexOption'



# 讀取資料夾中的所有 .rpt 檔案
for folder in os.listdir(folder_path)[-1:]:
    # folder = os.listdir(folder_path)[-1]
    if os.path.isdir(os.path.join(folder_path, folder)):
        for file in os.listdir(os.path.join(folder_path, folder)):
            # file = os.listdir(os.path.join(folder_path, folder))[0]
            if file.endswith('.rpt'):
                read_rpt_insert(file)
            elif file.endswith('.zip'):
                try:
                    zip_path = os.path.join(folder_path, folder, file)
                    unzip_file(zip_path, os.path.join(folder_path, folder))
                    rpt_file = file[:-4] + '.rpt'
                    read_rpt_insert(rpt_file)
                    os.remove( os.path.join(folder_path, folder, rpt_file))  
                except Exception as e:
                    print(f'Error: {e}')


            
# # 設置MongoDB連接
# client = MongoClient('mongodb://localhost:27017/')
# db = client['option_price']
# price_collection = db['future']

# # 指定資料夾路徑
# folder_path = './data/TaifexRPT'



# # 讀取資料夾中的所有 .rpt 檔案
# for folder in os.listdir(folder_path)[-3:]:
#     # folder = os.listdir(folder_path)[-1]
#     if os.path.isdir(os.path.join(folder_path, folder)):
#         for file in os.listdir(os.path.join(folder_path, folder)):
#             # file = os.listdir(os.path.join(folder_path, folder))[0]
#             if file.endswith('.rpt'):
#                 read_rpt_insert_future(file)
#             elif file.endswith('.zip'):
#                 try:
#                     zip_path = os.path.join(folder_path, folder, file)
#                     unzip_file(zip_path, os.path.join(folder_path, folder))
#                     rpt_file = file[:-4] + '.rpt'
#                     read_rpt_insert_future(rpt_file)
#                     os.remove( os.path.join(folder_path, folder, rpt_file))  
#                 except Exception as e:
#                     print(f'Error: {e}')