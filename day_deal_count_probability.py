# %%
import os

import pandas as pd

# 獲取 ./data/group 中的所有檔案名稱
file_folders = [
    os.path.splitext(file)[0]
    for file in os.listdir('./data/group')
    if file.endswith('.csv')
]

df_info = pd.DataFrame(
    columns=[
        'month',
        'underlayed',
        'kind',
        'strike_price',
        'MTF_PROD_ID',
        'previous_last_price',
        'last_first_price',
        'last_last_price',
        'previous_day',
        'last_day',
    ]
)
df_info = pd.DataFrame()
for file_folder in file_folders:
    # file_folder = file_folders[0]

    print(file_folder)

    month_data_df = pd.read_csv(f'data/group/{file_folder}.csv')
    month_data_df['年月日'] = pd.to_datetime(month_data_df['年月日'])

    # month3_data_df_16500 = month3_data_df[month3_data_df['strike_price']==16500]

    month_data_df.sort_values(by='履約價(元)', inplace=True)

    # # 列出所有日期
    # date_day = df_strike_price.resample('1D').first().dropna().index.date

    # 計算最後一天變動%數

    last_day_trades = month_data_df[month_data_df['剩餘交易日數'] == 1]
    previous_day_trades = month_data_df[month_data_df['剩餘交易日數'] == 2]

    # 將索引值變成一個新的欄位並重設索引
    last_day_trades.reset_index(inplace=True)
    previous_day_trades.reset_index(inplace=True)

    # 在每個欄位後前加上結算日或前天
    last_day_trades = last_day_trades.add_prefix('結算日')
    previous_day_trades = previous_day_trades.add_prefix('前天')

    # 合併欄位
    merged_df = pd.concat([last_day_trades, previous_day_trades], axis=1)
    merged_df.reset_index(drop=True, inplace=True)

    df_info = pd.concat([df_info, merged_df], axis=0)


df_info.reset_index(drop=True, inplace=True)


# # 讀取TX2-4.csv期貨價格
# TX_df = pd.read_csv('./data/index/TX2-4-1.csv', encoding='utf-8', sep='\t')
# TX_df['datetime'] = pd.to_datetime(TX_df['年月日'], format='%Y%m%d')
# TX_df.set_index('datetime', inplace=True)


# # 日期與期貨價格對應
# def get_data(date, df, column):
#     prices = df[(df.index == date)][column]
#     if not prices.empty:
#         return prices.iloc[0]
#     else:
#         return (
#             None  # Or any other value you want to use when there is no match
#         )


# 計算previous_last_first變動%
df_info['previous_last_day_change'] = (
    (df_info['結算日選擇權收盤價'] - df_info['前天選擇權收盤價'])
    / df_info['前天選擇權收盤價']
    * 100
)

# 計算last_first-last變動%
df_info['last_day_change'] = (
    (df_info['結算日選擇權收盤價'] - df_info['結算日選擇權開盤價'])
    / df_info['結算日選擇權開盤價']
    * 100
)


# 計算價內價外
df_info['moneyness'] = (
    df_info['結算日履約價(元)'] / df_info['前天標的證券價格']
)


# # 新增sigma
# vitx_df = pd.read_csv('./data/index/vitx.csv', encoding='utf-16', sep='\t')
# vitx_df['datetime'] = pd.to_datetime(vitx_df['年月日'], format='%Y%m%d')
# vitx_df.set_index('datetime', inplace=True)

# # 前一天sigma
# df_info['previous_day_sigma'] = df_info.apply(
#     lambda row: get_data(
#         row['previous_day'], vitx_df, column='收盤波動率指數'
#     ),
#     axis=1,
# )
# # 轉成小數點
# df_info['previous_day_sigma'] = df_info['previous_day_sigma'] / 100

# import numpy as np
# from scipy.stats import norm

# N = norm.cdf


# def BS_CALL(S, K, T, r, sigma):
#     d1 = (np.log(S / K) + (r + sigma**2 / 2) * T) / (sigma * np.sqrt(T))
#     d2 = d1 - sigma * np.sqrt(T)
#     return S * N(d1) - K * np.exp(-r * T) * N(d2)


# def BS_PUT(S, K, T, r, sigma):
#     d1 = (np.log(S / K) + (r + sigma**2 / 2) * T) / (sigma * np.sqrt(T))
#     d2 = d1 - sigma * np.sqrt(T)
#     return K * np.exp(-r * T) * N(-d2) - S * N(-d1)


# r = 0.00795
# t = 1 / 244
# df_info['previous_bs_price'] = df_info.apply(
#     lambda row: (
#         BS_CALL(
#             row['previous_day_index_price'],
#             row['strike_price'],
#             t,
#             r,
#             row['previous_day_sigma'],
#         )
#         if row['kind'] == 'C'
#         else BS_PUT(
#             row['previous_day_index_price'],
#             row['strike_price'],
#             t,
#             r,
#             row['previous_day_sigma'],
#         )
#     ),
#     axis=1,
# )


# 排序
df_info.sort_values(
    by=[
        '結算日year_month',
        '結算日underlayed',
        '結算日kind',
        '結算日履約價(元)',
    ],
    inplace=True,
)
df_info = df_info[[
    '結算日證券代碼',
    '結算日PRODID',
    '結算日underlayed',
    '結算日kind',
    '結算日選擇權開盤價',
    '結算日選擇權收盤價',
    '結算日選擇權結算價',
    '前天選擇權開盤價',
    '前天選擇權收盤價',
    '前天選擇權結算價',
    'moneyness',
    'previous_last_day_change',
    'last_day_change',
]]

df_info = df_info.rename(columns={
    '結算日證券代碼': '證券代碼',
    '結算日PRODID': 'PRODID',
    '結算日underlayed': 'underlayed',
    '結算日kind': 'kind',
})


# 周選
df_info[df_info['underlayed'].str.contains('W1|W2|W4|W5')].to_csv(
    './data/info/week_options_last_day_info.csv',
    index=False,
    encoding='utf-8-sig',
)

# 月選
df_info[~df_info['underlayed'].str.contains('W1|W2|W4|W5')].to_csv(
    './data/info/month_options_last_day_info.csv',
    index=False,
    encoding='utf-8-sig',
)

# 全部
# 周選
df_info.to_csv(
    './data/info/all_options_last_day_info.csv',
    index=False,
    encoding='utf-8-sig',
)


# %%