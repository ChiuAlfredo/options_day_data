# %%
import pandas as pd
import os
import util.count_util as count_util
import util.draw_util as draw_util

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

for file_folder in file_folders:
    # file_folder = file_folders[0]

    print(file_folder)

    month_data_df = pd.read_parquet(f'data/group/{file_folder}.gzip')
    month_data_df['datetime'] = pd.to_datetime(month_data_df['datetime'])
    month_data_df['MTF_DATE'] = pd.to_datetime(month_data_df['MTF_DATE'])

    # month3_data_df_16500 = month3_data_df[month3_data_df['strike_price']==16500]

    # 假設 df 是你的數據框，並且 'strike_price' 是其中的一個列
    for strike_price in month_data_df['strike_price'].unique():
        # strike_price = month_data_df['strike_price'].unique()[1]
        # strike_price  = 13500
        # print(strike_price)

        # 獲取當前 strike_price 的數據
        df_strike_price = month_data_df[
            month_data_df['strike_price'] == strike_price
        ]
        df_strike_price.set_index('datetime', inplace=True)

        # # 列出所有日期
        # date_day = df_strike_price.resample('1D').first().dropna().index.date

        # 計算最後一天變動%數
        (
            previous_last_trade,
            last_first_trade,
            last_last_trade,
            last_day,
            previous_day,
        ) = count_util.count_last_day_change(df_strike_price)

        df_dict = {
            'month': df_strike_price['month'].iloc[0],
            'underlayed': df_strike_price['underlayed'].iloc[0],
            'kind': df_strike_price['kind'].iloc[0],
            'strike_price': strike_price,
            'MTF_PROD_ID': df_strike_price['MTF_PROD_ID'].iloc[0],
            'previous_last_price': previous_last_trade,
            'last_first_price': last_first_trade,
            'last_last_price': last_last_trade,
            'previous_day': previous_day,
            'last_day': last_day,
        }

        # 創建兩個新的 DataFrame，並將 df_dict的值分別添加到新的行
        new_row_df = pd.DataFrame(df_dict, index=[0])

        new_row_df = new_row_df.dropna(how='all')

        # 將新的行添加到 df_info
        df_info = pd.concat([df_info, new_row_df])

df_info.reset_index(drop=True, inplace=True)

# 轉換日期格式
df_info['previous_day'] = pd.to_datetime(df_info['previous_day'])
df_info['last_day'] = pd.to_datetime(df_info['last_day'])

## 補齊所有前一天沒有交易的資料

# 找到所有相同標地的資料
df_info_group = df_info.groupby(['month', 'underlayed'])

# 找到最後的previous_day並添加到所有相同標地的資料
latest_previous_day = df_info_group['previous_day'].transform('max')
df_info['previous_day'] = latest_previous_day

# 找到最後的previous_day並添加到所有相同標地的資料
latest_last_day = df_info_group['last_day'].transform('max')
df_info['last_day'] = latest_last_day


# 讀取TX2-4.csv期貨價格
TX_df = pd.read_csv('./data/index/TX2-4-1.csv', encoding='utf-8', sep='\t')
TX_df['datetime'] = pd.to_datetime(TX_df['年月日'], format='%Y%m%d')
TX_df.set_index('datetime', inplace=True)


# 日期與期貨價格對應
def get_data(date, df, column):
    prices = df[(df.index == date)][column]
    if not prices.empty:
        return prices.iloc[0]
    else:
        return (
            None  # Or any other value you want to use when there is no match
        )


# 前一天期貨收盤價
df_info['previous_day_index_price'] = df_info.apply(
    lambda row: get_data(row['previous_day'], TX_df, column='收盤價(元)'),
    axis=1,
)

# 前一天期貨收盤價
df_info['last_day_index_price'] = df_info.apply(
    lambda row: get_data(row['last_day'], TX_df, column='收盤價(元)'), axis=1
)

# 計算previous_last_first變動%
df_info['previous_last_day_change'] = (
    (df_info['last_last_price'] - df_info['previous_last_price'])
    / df_info['previous_last_price']
    * 100
)

# 計算last_first-last變動%
df_info['last_day_change'] = (
    (df_info['last_last_price'] - df_info['last_first_price'])
    / df_info['last_first_price']
    * 100
)


# 計算價內價外
df_info['moneyness'] = (
    df_info['strike_price'] / df_info['previous_day_index_price']
)


# 新增sigma
vitx_df = pd.read_csv('./data/index/vitx.csv', encoding='utf-16', sep='\t')
vitx_df['datetime'] = pd.to_datetime(vitx_df['年月日'], format='%Y%m%d')
vitx_df.set_index('datetime', inplace=True)

# 前一天sigma
df_info['previous_day_sigma'] = df_info.apply(
    lambda row: get_data(
        row['previous_day'], vitx_df, column='收盤波動率指數'
    ),
    axis=1,
)
# 轉成小數點
df_info['previous_day_sigma'] = df_info['previous_day_sigma'] / 100

import numpy as np
from scipy.stats import norm

N = norm.cdf


def BS_CALL(S, K, T, r, sigma):
    d1 = (np.log(S / K) + (r + sigma**2 / 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return S * N(d1) - K * np.exp(-r * T) * N(d2)


def BS_PUT(S, K, T, r, sigma):
    d1 = (np.log(S / K) + (r + sigma**2 / 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return K * np.exp(-r * T) * N(-d2) - S * N(-d1)


r = 0.00795
t = 1 / 244
df_info['previous_bs_price'] = df_info.apply(
    lambda row: (
        BS_CALL(
            row['previous_day_index_price'],
            row['strike_price'],
            t,
            r,
            row['previous_day_sigma'],
        )
        if row['kind'] == 'C'
        else BS_PUT(
            row['previous_day_index_price'],
            row['strike_price'],
            t,
            r,
            row['previous_day_sigma'],
        )
    ),
    axis=1,
)


# 排序
df_info.sort_values(
    by=['month', 'underlayed', 'kind', 'strike_price'], inplace=True
)

# 周選
df_info[df_info['underlayed'].str.contains('TX1|TX2|TX4|TX5')].to_csv(
    './data/info/week_options_last_day_info.csv',
    index=False,
    encoding='utf-8-sig',
)

# 月選
df_info[df_info['underlayed'].str.contains('TXO')].to_csv(
    './data/info/month_options_last_day_info.csv',
    index=False,
    encoding='utf-8-sig',
)


# %%
# 繪製變動%圖表
file_folder = 'prob'
# 檢查 file_folder 是否存在於 ./data/picture/
if not os.path.exists(f'./data/picture/{file_folder}'):
    # 建立新的資料夾
    os.makedirs(f'./data/picture/{file_folder}', exist_ok=True)


# 繪圖
def draw_prop_change(df_info, df_title):

    draw_util.draw_prop_change(
        df_info=df_info,
        column='previous_last_day_change',
        title=f'{df_title}_prob_previous_last',
    )

    # C
    draw_util.draw_prop_change(
        df_info=df_info[(df_info['moneyness'] < 0.97) & (df_info['kind']=='C')],
        column='previous_last_day_change',
        title=f'{df_title}_deep_ITM_call_previous_last_change',
    )

    draw_util.draw_prop_change(
        df_info=df_info[(df_info['moneyness'] >= 0.97) &(df_info['moneyness']<0.99)& (df_info['kind']=='C')],
        column='previous_last_day_change',
        title=f'{df_title}_ITM_call_previous_last_change',
    )

    draw_util.draw_prop_change(
        df_info=df_info[(df_info['moneyness'] >= 0.99) &(df_info['moneyness']<1.01)& (df_info['kind']=='C')],
        column='previous_last_day_change',
        title=f'{df_title}_ATM_call_previous_last_change',
    )
    draw_util.draw_prop_change(
        df_info=df_info[(df_info['moneyness'] >= 1.01) &(df_info['moneyness']<1.03)& (df_info['kind']=='C')],
        column='previous_last_day_change',
        title=f'{df_title}_OTM_call_previous_last_change',
    )
    draw_util.draw_prop_change(
        df_info=df_info[(df_info['moneyness'] >= 1.03) & (df_info['kind']=='C')],
        column='previous_last_day_change',
        title=f'{df_title}_deep_OTM_call_previous_last_change',
    )

    # P
    draw_util.draw_prop_change(
        df_info=df_info[(df_info['moneyness'] < 0.97) & (df_info['kind']=='P')],
        column='previous_last_day_change',
        title=f'{df_title}_deep_OTM_put_previous_last_change',
    )

    draw_util.draw_prop_change(
        df_info=df_info[(df_info['moneyness'] >= 0.97) &(df_info['moneyness']<0.99)& (df_info['kind']=='P')],
        column='previous_last_day_change',
        title=f'{df_title}_OTM_put_previous_last_change',
    )

    draw_util.draw_prop_change(
        df_info=df_info[(df_info['moneyness'] >= 0.99) &(df_info['moneyness']<1.01)& (df_info['kind']=='P')],
        column='previous_last_day_change',
        title=f'{df_title}_ATM_put_previous_last_change',
    )
    draw_util.draw_prop_change(
        df_info=df_info[(df_info['moneyness'] >= 1.01) &(df_info['moneyness']<1.03)& (df_info['kind']=='P')],
        column='previous_last_day_change',
        title=f'{df_title}_ITM_put_previous_last_change',
    )
    draw_util.draw_prop_change(
        df_info=df_info[(df_info['moneyness'] >= 1.03) & (df_info['kind']=='P')],
        column='previous_last_day_change',
        title=f'{df_title}_deep_ITM_put_previous_last_change',
    )


    draw_util.draw_prop_change(
        df_info=df_info[
            df_info['previous_last_price'] > df_info['previous_bs_price']
        ],
        column='previous_last_day_change',
        title=f'{df_title}_prob_previous_last_price>bs',
    )
    draw_util.draw_prop_change(
        df_info=df_info[
            df_info['previous_last_price'] < df_info['previous_bs_price']
        ],
        column='previous_last_day_change',
        title=f'{df_title}_prob_previous_last_price<bs',
    )


# 月選
month_options_df = pd.read_csv(
    './data/info/month_options_last_day_info.csv', encoding='utf-8'
)

draw_prop_change(df_info=month_options_df, df_title='month')

# 周選
week_options_df = pd.read_csv(
    './data/info/week_options_last_day_info.csv', encoding='utf-8'
)

draw_prop_change(df_info=week_options_df, df_title='week')
