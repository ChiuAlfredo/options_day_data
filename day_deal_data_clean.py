# %%
import pandas as pd

# 欄位名稱列表
all_data_df = pd.read_csv('選擇權.txt',sep='\t')


# %%
# 選擇權標的
all_data_df['PRODID'] = all_data_df['證券代碼'].str.split('TSE').str[0]

# 只取履約價格之前的參數
all_data_df['underlayed'] = all_data_df['PRODID'].str.split('C').str[0].str.split('P').str[0]

# %%
# 新增履約月份欄位
all_data_df['year_month'] = all_data_df['underlayed'].str.slice(3, 9)

# C or P
all_data_df['kind'] = all_data_df['PRODID'].apply(lambda x: 'C' if 'C' in x else ('P' if 'P' in x else ''))



# %%

# no dask


# %%

# 將datetime欄位的數據轉換為日期時間格式
all_data_df['年月日'] = pd.to_datetime(
    all_data_df['年月日'], format='%Y%m%d'
)
# all_data_df['MTF_ORIG_TIME'] = pd.to_datetime(all_data_df['MTF_ORIG_TIME'], format='%H:%M:%S.%f')


# %%

# no dask
# 選擇要保留的欄位
# all_data_df = all_data_df[['MTF_DATE', 'MTF_PROD_ID', 'MTF_BS_CODE', 'MTF_PRICE', 'MTF_QNTY', 'MTF_OC_CODE', 'strike_price', 'month_alpha', 'month', 'datetime','kind']]
all_data_df['選擇權高估(元)'] = pd.to_numeric(all_data_df['選擇權高估(元)'], errors='coerce')
all_data_df['溢價比率%'] = pd.to_numeric(all_data_df['溢價比率%'], errors='coerce')

# dask
# %%
# MTF_PROD_ID分群
prod_df = all_data_df.groupby(['underlayed', 'kind'])

def write_group(group):
    month = group['year_month'].iloc[0]
    kind = group['kind'].iloc[0]
    underlayed = group['underlayed'].iloc[0]  # Get the first 'underlayed' value in the group
    filename = f'./data/group/{month}_{kind}_{underlayed}' 
    # group.to_parquet(f'{filename}.gzip', compression='gzip', index=False)
    group.to_csv(f'{filename}.csv', index=False, encoding='utf-8-sig')
    return pd.DataFrame()  # Return an empty DataFrame

# Apply the function to each group
prod_df = prod_df.apply(write_group)

