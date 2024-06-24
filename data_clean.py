# %%
# 解壓縮檔案
def unzip_files(source_dir, dest_dir):
    for item in os.listdir(source_dir):  # loop through items in dir
        if item.endswith('.zip'):  # check for ".zip" extension
            file_name = source_dir + "/" + item  # get full path of files
            zip_ref = zipfile.ZipFile(file_name)  # create zipfile object
            zip_ref.extractall(dest_dir)  # extract file to dir
            zip_ref.close()  # close file
            # os.remove(file_name) # delete zipped file
        else:
            continue

def main():
    # 解壓縮所有的資料並移動到./data
    # 選定zip資料夾
    source_dir = './選擇權日內成交檔資料11002-11004'
    dest_dir = './data/raw_data'
    unzip_files(source_dir, dest_dir)


    # 計算時間
    import time

    start_time = time.time()

    # 欄位名稱列表
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

    all_data_df = dd.read_csv(
        './data/raw_data/*.txt',
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
    all_data_df.compute()





    # %%
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
    alpha_to_month = { 'A': 1, 'B': 2, 'C': 3, 'D': 4, 'E': 5, 'F': 6, 'G': 7, 'H': 8, 'I': 9, 'J': 10, 'K': 11, 'L': 12, 'M': 1, 'N': 2, 'O': 3, 'P': 4, 'Q': 5, 'R': 6, 'S': 7, 'T': 8, 'U': 9, 'V': 10, 'W': 11, 'X': 12, }
    # no dask
    # # 轉換month_alpha
    # all_data_df['month'] = all_data_df['month_alpha'].map(alpha_to_month)

    # # 轉換month為字串
    # all_data_df['month'] = all_data_df['month'].astype(int).astype(str)

    # dask
    # 轉換month_alpha
    all_data_df['month'] = all_data_df['month_alpha'].map(alpha_to_month)


    # 轉換month為字串
    all_data_df['month'] = all_data_df['month'].astype(str)


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
    all_data_df['datetime'] = dd.to_datetime(
        all_data_df['datetime'], format='%Y%m%d %H:%M:%S.%f'
    )
    all_data_df['MTF_DATE'] = dd.to_datetime(
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


    all_data_df.compute()


    end_time = time.time()
    print("It cost %f sec" % (end_time - start_time))
    # %%
    # MTF_PROD_ID分群
    prod_df = all_data_df.groupby(['month_alpha', 'underlayed'])


    def write_group(group):
        month = group['month'].iloc[0]
        kind = group['kind'].iloc[0]
        underlayed = group['underlayed'].iloc[0]  # Get the first 'underlayed' value in the group
        filename = f'./data/group_day/{month}_{kind}_{underlayed}' 
        if month in ['2', '3', '4']:
            group.to_parquet(f'{filename}.gzip', compression='gzip', index=False)
            group.to_csv(f'{filename}.csv', index=False, encoding='utf-8-sig')
        return pd.DataFrame()  # Return an empty DataFrame

    # Apply the function to each group
    prod_df = prod_df.apply(write_group, meta=('month_alpha', 'object')).compute()

if __name__ == '__main__':
    from dask.distributed import LocalCluster

    import os
    import shutil
    import zipfile

    import pandas as pd
    import glob
    import dask.dataframe as dd
    cluster = LocalCluster()
    client = cluster.get_client()

    print(client)
    main()
from dask.distributed import LocalCluster
from distributed import Client
cluster = LocalCluster(processes=False)
client = cluster.get_client()
