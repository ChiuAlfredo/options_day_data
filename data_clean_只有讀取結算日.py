import glob
import os
import shutil
import zipfile

import pandas as pd


def unzip_files(source_dir, target_dir):
    for item in os.listdir(source_dir):  # loop through items in dir
        if item.endswith('.zip'):  # check for ".zip" extension
            file_name = source_dir + "/" + item  # get full path of files
            zip_ref = zipfile.ZipFile(file_name)  # create zipfile object
            zip_ref.extractall(target_dir)  # extract file to dir
            zip_ref.close()  # close file
            # os.remove(file_name) # delete zipped file
        else:
            continue

def build_folder(source_dir,destanation_dir):
    # 假設source_dir是一個路徑字符串
    source_dir_name = os.path.basename(source_dir)  # 獲取source_dir的名字
    target_dir = os.path.join(destanation_dir, source_dir_name)  # 拼接目標路徑

    # 檢查目標路徑是否存在，如果不存在則創建資料夾
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
        print(f"Created directory: {target_dir}")
    else:
        print(f"Directory already exists: {target_dir}")

    return target_dir

source_dir = './成交檔-選擇權10906-11004'

destanation_dir = './data/raw_data'
target_dir = build_folder(source_dir,destanation_dir)
unzip_files(source_dir, target_dir)

df_end_date_info = pd.read_excel('結算日日期和種類.xlsx')

# 將月份和日期轉換為兩位數格式
df_end_date_info['月'] = df_end_date_info['月'].astype(str).str.zfill(2)
df_end_date_info['日'] = df_end_date_info['日'].astype(str).str.zfill(2)

for index, row in df_end_date_info.iterrows():
    # row = df_end_date_info.iloc[0]

    year = row['年']
    month = row['月']
    day = row['日']
    if year != '2021':
        continue
    file_name = f'OWMTF_{year}{month}{day}.txt'
    file_dir = os.path.join(target_dir, file_name)

    column_names = [
        'MTF_DATE',
        'MTF_PROD_ID',
        'MTF_PROD_ID1',
        'MTF_BS_CODE1',
        'MTF_M_PRICE1',
        'MTF_M_QNTY1',
        'MTF_PROD_ID2',
        'MTF_BS_CODE2',
        'MTF_M_PRICE2',
        'MTF_M_QNTY2',
        'MTF_BS_CODE',
        'MTF_PRICE',
        'MTF_QNTY',
        'MTF_OC_CODE',
        'MTF_M_INST',
        'MTF_ORIG_TIME',
        'MTF_SC_CODE',
        'FCM_NO_ORDER_NO_',
        'OQ_CODE',
    ]

    all_data_df = pd.read_csv(
        file_dir,
        header=None,
        names=column_names,
        sep='\t',
        dtype={
            'MTF_DATE': 'object',
            'MTF_PROD_ID': 'object',
            'MTF_PROD_ID1': 'object',
            'MTF_BS_CODE1': 'object',
            'MTF_M_PRICE1': 'float64',  # Change this to 'float64'
            'MTF_M_QNTY1': 'float64',  # And this
            'MTF_PROD_ID2': 'object',
            'MTF_BS_CODE2': 'object',
            'MTF_M_PRICE2': 'float64',  # And this
            'MTF_M_QNTY2': 'float64',  # And this
            'MTF_BS_CODE': 'object',
            'MTF_PRICE': 'float64',
            'MTF_QNTY': 'float64',  # And this
            'MTF_OC_CODE': 'object',
            'MTF_M_INST': 'object',
            'MTF_ORIG_TIME': 'object',
            'MTF_SC_CODE': 'object',
            'FCM_NO_ORDER_NO_': 'object',
            'OQ_CODE': 'object',
        },
        engine='python'

    )
    # 只留下MTD_PROD_ID含有TX的台指期選擇權
    all_data_df = all_data_df[all_data_df['MTF_PROD_ID'].str.contains('TXO|TX1|TX2|TX4|TX5', na=False)]

    # 留下單式商品
    all_data_df = all_data_df[all_data_df['MTF_SC_CODE'] == 'S']

    # 刪除'MTF_PROD_ID'欄位中的空格
    all_data_df['MTF_PROD_ID'] = all_data_df['MTF_PROD_ID'].str.replace(' ', '')

    # 選擇權標的
    all_data_df['underlayed'] = all_data_df['MTF_PROD_ID'].str.slice(0, 3)


    # %%
    # 新增履約價格欄位
    all_data_df['strike_price'] = all_data_df['MTF_PROD_ID'].str.slice(3, 8)
    all_data_df['strike_price'] = all_data_df['strike_price'].astype(int)

    # %%
    # 新增履約月份欄位
    all_data_df['month_alpha'] = all_data_df['MTF_PROD_ID'].str.slice(8, 9)

    # 月份映射Ａ~L(112月)買權,M~X(112月)賣權
    alpha_to_month = { 'A': '01', 'B': '02', 'C': '03', 'D': '04', 'E': '05', 'F': '06', 'G': '07', 'H': '08', 'I': '09', 'J': '10', 'K': '11', 'L': '12', 'M': '01', 'N': '02', 'O': '03', 'P': '04', 'Q': '05', 'R': '06', 'S': '07', 'T': '08', 'U': '09', 'V': '10', 'W': '11', 'X': '12', }
    # no dask
    # # 轉換month_alpha
    # all_data_df['month'] = all_data_df['month_alpha'].map(alpha_to_month)

    # # 轉換month為字串
    # all_data_df['month'] = all_data_df['month'].astype(int).astype(str)

    # dask
    # 轉換month_alpha
    all_data_df['month'] = all_data_df['month_alpha'].map(alpha_to_month)


    # 轉換month為字串
    # all_data_df['month'] = all_data_df['month'].astype(str)


    # %%
    # 新增選擇權買賣權欄位
    alpha_to_kind = {char: 'C' for char in 'ABCDEFGHIJKL'}
    alpha_to_kind.update({char: 'P' for char in 'MNOPQRSTUVWX'})

    # no dask
    all_data_df['kind'] = all_data_df['month_alpha'].map(alpha_to_kind)




    # %%
    # 將MTF_DATE和MTF_ORIG_TIME欄位的數據轉換為字串格式
    all_data_df['MTF_DATE'] = all_data_df['MTF_DATE'].astype(str)
    all_data_df['MTF_ORIG_TIME'] = all_data_df['MTF_ORIG_TIME'].astype(str)

    # 合併MTF_DATE和MTF_ORIG_TIME欄位的數據
    all_data_df['datetime'] = (
        all_data_df['MTF_DATE'] + ' ' + all_data_df['MTF_ORIG_TIME']
    )

    # 將datetime欄位的數據轉換為日期時間格式
    all_data_df['datetime'] = pd.to_datetime(
        all_data_df['datetime'], format='%Y%m%d %H:%M:%S.%f'
    )
    all_data_df['MTF_DATE'] = pd.to_datetime(
        all_data_df['MTF_DATE'], format='%Y%m%d'
    )
    # all_data_df['MTF_ORIG_TIME'] = pd.to_datetime(all_data_df['MTF_ORIG_TIME'], format='%H:%M:%S.%f')


    # %%

    # no dask
    # 選擇要保留的欄位
    # all_data_df = all_data_df[['MTF_DATE', 'MTF_PROD_ID', 'MTF_BS_CODE', 'MTF_PRICE', 'MTF_QNTY', 'MTF_OC_CODE', 'strike_price', 'month_alpha', 'month', 'datetime','kind']]

    # dask
    all_data_df = all_data_df[
        [
            'MTF_DATE',
            'MTF_PROD_ID',
            'MTF_BS_CODE',
            'MTF_PRICE',
            'MTF_QNTY',
            'MTF_OC_CODE',
            'strike_price',
            'month_alpha',
            'month',
            'datetime',
            'kind',
            'underlayed'
        ]
    ]

    # 只留下當天結算
    all_data_df = all_data_df[(all_data_df['month']==row['月'])&(all_data_df['underlayed']==row['種類'])]


    data_dir = build_folder(f'','./data/選擇權結算/')


    all_data_df.to_parquet(f'{data_dir}{row["年"]}_{row["月"]}_{row["日"]}_{row["種類"]}.gzip', compression='gzip', index=False)
    all_data_df.to_csv(f'{data_dir}{row["年"]}_{row["月"]}_{row["日"]}_{row["種類"]}.csv', index=False, encoding='utf-8-sig')

#%%

source_dir = './成交檔-期貨10906-11004'

destanation_dir = './data/raw_data'
target_dir = build_folder(source_dir,destanation_dir)
unzip_files(source_dir, target_dir)

df_end_date_info = pd.read_excel('結算日日期和種類.xlsx')

# 將月份和日期轉換為兩位數格式
df_end_date_info['月'] = df_end_date_info['月'].astype(str).str.zfill(2)
df_end_date_info['日'] = df_end_date_info['日'].astype(str).str.zfill(2)

for index, row in df_end_date_info.iterrows():
    # row = df_end_date_info.iloc[0]

    year = row['年']
    month = row['月']
    day = row['日']
    file_name = f'FWMTF_{year}{month}{day}.txt'
    file_dir = os.path.join(target_dir, file_name)

    column_names = [
        'MTF_DATE',
        'MTF_PROD_ID',
        'MTF_PROD_ID1',
        'MTF_BS_CODE1',
        'MTF_M_PRICE1',
        'MTF_M_QNTY1',
        'MTF_PROD_ID2',
        'MTF_BS_CODE2',
        'MTF_M_PRICE2',
        'MTF_M_QNTY2',
        'MTF_BS_CODE',
        'MTF_PRICE',
        'MTF_QNTY',
        'MTF_OC_CODE',
        'MTF_M_INST',
        'MTF_ORIG_TIME',
        'MTF_SC_CODE',
        'FCM_NO_ORDER_NO_',
        'OQ_CODE',
    ]

    all_data_df = pd.read_csv(
        file_dir,
        header=None,
        names=column_names,
        sep='\t',
        dtype={
            'MTF_DATE': 'object',
            'MTF_PROD_ID': 'object',
            'MTF_PROD_ID1': 'object',
            'MTF_BS_CODE1': 'object',
            'MTF_M_PRICE1': 'float64',  # Change this to 'float64'
            'MTF_M_QNTY1': 'float64',  # And this
            'MTF_PROD_ID2': 'object',
            'MTF_BS_CODE2': 'object',
            # 'MTF_M_PRICE2': 'float64',  # And this
            # 'MTF_M_QNTY2': 'float64',  # And this
            # 'MTF_BS_CODE': 'object',
            # 'MTF_PRICE': 'float64',
            # 'MTF_QNTY': 'float64',  # And this
            # 'MTF_OC_CODE': 'object',
            # 'MTF_M_INST': 'object',
            'MTF_ORIG_TIME': 'object',
            'MTF_SC_CODE': 'object',
            'FCM_NO_ORDER_NO_': 'object',
            'OQ_CODE': 'object',
        },
        engine='python'

    )
    # 只留下MTD_PROD_ID含有TX的台指期選擇權
    all_data_df = all_data_df[all_data_df['MTF_PROD_ID'].str.contains('TXF', na=False)]

    # 留下單式商品
    all_data_df = all_data_df[all_data_df['MTF_SC_CODE'] == 'S']

    # 刪除'MTF_PROD_ID'欄位中的空格
    all_data_df['MTF_PROD_ID'] = all_data_df['MTF_PROD_ID'].str.replace(' ', '')

    # # 選擇權標的
    # all_data_df['underlayed'] = all_data_df['MTF_PROD_ID'].str.slice(0, 3)


    # %%
    # # 新增履約價格欄位
    # all_data_df['strike_price'] = all_data_df['MTF_PROD_ID'].str.slice(3, 8)
    # all_data_df['strike_price'] = all_data_df['strike_price'].astype(int)

    # # %%
    # # 新增履約月份欄位
    # all_data_df['month_alpha'] = all_data_df['MTF_PROD_ID'].str.slice(8, 9)

    # # 月份映射Ａ~L(112月)買權,M~X(112月)賣權
    # alpha_to_month = { 'A': '01', 'B': '02', 'C': '03', 'D': '04', 'E': '05', 'F': '06', 'G': '07', 'H': '08', 'I': '09', 'J': '10', 'K': '11', 'L': '12', 'M': '01', 'N': '02', 'O': '03', 'P': '04', 'Q': '05', 'R': '06', 'S': '07', 'T': '08', 'U': '09', 'V': '10', 'W': '11', 'X': '12', }
    # # no dask
    # # # 轉換month_alpha
    # # all_data_df['month'] = all_data_df['month_alpha'].map(alpha_to_month)

    # # # 轉換month為字串
    # # all_data_df['month'] = all_data_df['month'].astype(int).astype(str)

    # # dask
    # # 轉換month_alpha
    # all_data_df['month'] = all_data_df['month_alpha'].map(alpha_to_month)


    # # 轉換month為字串
    # # all_data_df['month'] = all_data_df['month'].astype(str)


    # # %%
    # # 新增選擇權買賣權欄位
    # alpha_to_kind = {char: 'C' for char in 'ABCDEFGHIJKL'}
    # alpha_to_kind.update({char: 'P' for char in 'MNOPQRSTUVWX'})

    # # no dask
    # all_data_df['kind'] = all_data_df['month_alpha'].map(alpha_to_kind)




    # %%
    # 將MTF_DATE和MTF_ORIG_TIME欄位的數據轉換為字串格式
    all_data_df['MTF_DATE'] = all_data_df['MTF_DATE'].astype(str)
    all_data_df['MTF_ORIG_TIME'] = all_data_df['MTF_ORIG_TIME'].astype(str)

    # 合併MTF_DATE和MTF_ORIG_TIME欄位的數據
    all_data_df['datetime'] = (
        all_data_df['MTF_DATE'] + ' ' + all_data_df['MTF_ORIG_TIME']
    )

    # 將datetime欄位的數據轉換為日期時間格式
    all_data_df['datetime'] = pd.to_datetime(
        all_data_df['datetime'], format='%Y%m%d %H:%M:%S.%f'
    )
    all_data_df['MTF_DATE'] = pd.to_datetime(
        all_data_df['MTF_DATE'], format='%Y%m%d'
    )
    # all_data_df['MTF_ORIG_TIME'] = pd.to_datetime(all_data_df['MTF_ORIG_TIME'], format='%H:%M:%S.%f')


    # %%

    # no dask
    # 選擇要保留的欄位
    # all_data_df = all_data_df[['MTF_DATE', 'MTF_PROD_ID', 'MTF_BS_CODE', 'MTF_PRICE', 'MTF_QNTY', 'MTF_OC_CODE', 'strike_price', 'month_alpha', 'month', 'datetime','kind']]

    # dask
    all_data_df = all_data_df[
        [
            'MTF_DATE',
            'MTF_PROD_ID',
            'MTF_BS_CODE',
            'MTF_PRICE',
            'MTF_QNTY',
            'MTF_OC_CODE',
            # 'strike_price',
            # 'month_alpha',
            # 'month',
            'datetime',
            # 'kind',
            # 'underlayed'
        ]
    ]

    # # 只留下當天結算
    # all_data_df = all_data_df[(all_data_df['month']==row['月'])&(all_data_df['underlayed']==row['種類'])]


    data_dir = build_folder(f'','./data/期貨結算/')


    all_data_df.to_parquet(f'{data_dir}{row["年"]}_{row["月"]}_{row["日"]}.gzip', compression='gzip', index=False)
    # all_data_df.to_csv(f'{data_dir}{row["年"]}_{row["月"]}_{row["日"]}.csv', index=False, encoding='utf-8-sig')


    
