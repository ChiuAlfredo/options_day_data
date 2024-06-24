import os
from util.BS_util import BS_formula
import pandas as pd
from functools import reduce


def read_index(year, month, day, kind):
    df_index = pd.read_csv(
        f'data/指數/{year}{month}{day}_{kind}.csv', encoding='utf-8-sig'
    )
    df_index['發行量加權股價指數'] = (
        df_index['發行量加權股價指數']
        .replace(',', '', regex=True)
        .astype(float)
    )

    # 將 df_index['時間'] 的日期部分替換為最大日期
    df_index['時間'] = year + ":" + month + ":" + day + ":" + df_index['時間']
    df_index['時間'] = pd.to_datetime(
        df_index['時間'], format='%Y:%m:%d:%H:%M:%S'
    )

    endtime = pd.Timestamp(
        year=df_index['時間'].max().year,
        month=df_index['時間'].max().month,
        day=df_index['時間'].max().day,
        hour=13,
        minute=30,
    )

    df_index['time'] = (
        (endtime - df_index['時間']) / (endtime - df_index['時間'].min()) / 252
    )

    # 取樣間隔為五筆資料
    df_index_new = df_index.iloc[::60]
    df_index_new = df_index_new[:-1]
    
    df_index_new = pd.concat([df_index_new,df_index.iloc[-5:]])

    df_index_new = df_index_new.iloc[:-4]
    

    df_index_new = df_index_new[['時間','發行量加權股價指數','time']]
    df_index = df_index[['時間','發行量加權股價指數','time']]

    df_index_new.reset_index(drop=True, inplace=True)

    return df_index,df_index_new


def read_option_price(file_path):
    month_data_df = pd.read_parquet(file_path)
    month_data_df['datetime'] = pd.to_datetime(month_data_df['datetime'])
    month_data_df['MTF_DATE'] = pd.to_datetime(month_data_df['MTF_DATE'])

    begin_time  =   pd.Timestamp(
        year=month_data_df['datetime'].max().year,
        month=month_data_df['datetime'].max().month,
        day=month_data_df['datetime'].max().day,
        hour=00,
        minute=00,
    )
    end_time = pd.Timestamp(
        year=month_data_df['datetime'].max().year,  
        month=month_data_df['datetime'].max().month,
        day=month_data_df['datetime'].max().day,
        hour=13,
        minute=30,
    )
    month_data_df = month_data_df[(month_data_df['datetime']>=begin_time)& (month_data_df['datetime']<=end_time)]
    
    month_data_df = month_data_df[month_data_df['MTF_BS_CODE']=='B']
    return month_data_df

def get_delta(month_index,strike_price_list):
    df_delta = pd.DataFrame()
    for k in strike_price_list:
        # S = df_merged['發行量加權股價指數'][0]
        r = 0.00795

        # K = 16300
        # T = df_merged['time'][0]
        sigma = 0.209223
        

        df_delta[f'{k}_delta'] = month_index.apply(
            lambda row: (
                BS_formula(
                    row['發行量加權股價指數'],
                    k,
                    r,
                    sigma,
                    row['time'],
                )
                ).BS_delta()[0]
            ,    axis=1,
        )

    df_delta.index = month_index['時間']

    return df_delta


def get_gamma(month_index,strike_price_list):
    df_gamma = pd.DataFrame()
    for k in strike_price_list:
        # S = df_merged['發行量加權股價指數'][0]
        r = 0.00795

        # K = 16300
        # T = df_merged['time'][0]
        sigma = 0.209223
        

        df_gamma[f'{k}_gamma'] = month_index.apply(
            lambda row: (
                BS_formula(
                    row['發行量加權股價指數'],
                    k,
                    r,
                    sigma,
                    row['time'],
                )
                ).BS_gamma()[0]
            ,    axis=1,
        )

    df_gamma.index = month_index['時間']

    return df_gamma


def get_bs_price_c(month_index,strike_price_list):
    df_bs_price_c = pd.DataFrame()
    df_bs_price_p = pd.DataFrame()
    for k in strike_price_list:
        # S = df_merged['發行量加權股價指數'][0]
        r = 0.00795

        # K = 16300
        # T = df_merged['time'][0]
        sigma = 0.209223
        

        df_bs_price_c[f'{k}_bs_C'] = month_index.apply(
            lambda row: (
                BS_formula(
                    row['發行量加權股價指數'],
                    k,
                    r,
                    sigma,
                    row['time'],
                )
            ).BS_price()[0]
            , axis=1,
        )

        df_bs_price_p[f'{k}_bs_P'] = month_index.apply(
            lambda row: (
                BS_formula(
                    row['發行量加權股價指數'],
                    k,
                    r,
                    sigma,
                    row['time'],
                )
            ).BS_price()[1]
            , axis=1,
        )

    df_bs_price_c.index = month_index['時間']
    df_bs_price_p.index = month_index['時間']
    
    return df_bs_price_c,df_bs_price_p

def get_option_price(month_price,strike_price_list):

    df_price =pd.DataFrame()
    df_price['時間']= month_index['時間']
    for k in strike_price_list:
        # k = strike_price_list[0]

        df = month_price[month_price['strike_price'] == k]
        df_price[f'{k}_成交量'] = 0
        df_price[f'{k}_成交價'] = 0

        df_price[f'{k}_成交量'].astype(float)
        df_price[f'{k}_成交價'].astype(float)
        for t in range(len(df_price)):
            # print(t)
            # t = 24
            if t ==0:
                df_t = df[df['datetime']<= df_price['時間'][t]]
            
            else:
                df_t = df[(df['datetime']<= df_price['時間'][t])&(df['datetime']> df_price['時間'][t-1])]
            if df_t.empty:
                df_price.at[t, f'{k}_成交量'] = 0
                df_price.at[t, f'{k}_成交價'] = 0
            else:
                price = (df_t['MTF_PRICE']*df_t['MTF_QNTY']).sum()/df_t['MTF_QNTY'].sum()
                quantity = df_t['MTF_QNTY'].sum()

                df_price.at[t, f'{k}_成交量'] = round(quantity, 3)
                df_price.at[t, f'{k}_成交價'] = round(price, 3)
            
    return df_price


year = '2021'
month = '03'
day = '03'
kind = 'TX1'


month_price_c = read_option_price(f'data/group/3_C_TX1.gzip')
month_price_p = read_option_price(f'data/group/3_P_TX1.gzip')
month_index = read_index(year, month, day, kind)[1]

price_range = {
    'min': round(month_index['發行量加權股價指數'].mean() / 100) * 100 - 300,
    'max': round(month_index['發行量加權股價指數'].mean() / 100) * 100 + 300,
    'step': 100,
}

strike_price_list = sorted(month_price_c[
        (month_price_c['strike_price'] < price_range['max'])
        & (month_price_c['strike_price'] > price_range['min'])
    ]['strike_price'].unique().tolist())




df_delta = get_delta(month_index,strike_price_list)
df_gamma = get_gamma(month_index,strike_price_list)
df_bs_price_c,df_bs_price_p = get_bs_price_c(month_index,strike_price_list)



df_price_c = get_option_price(month_price_c,strike_price_list).set_index('時間')
df_price_p = get_option_price(month_price_p,strike_price_list).set_index('時間')

df_price_c.columns = [f"{col}_C" for col in df_price_c.columns]
df_price_p.columns = [f"{col}_P" for col in df_price_p.columns]


month_index


# List of dataframes to merge
dfs = [df_delta, df_gamma, df_bs_price_c, df_bs_price_p, df_price_c, df_price_p, month_index]

# Use reduce to merge all dataframes

month_data_df_5min = reduce(lambda left,right: pd.merge(left,right,on='時間', how='outer'), dfs)
month_data_df_5min.to_csv(f'202103TX1每五分鐘資料.csv',index=False,encoding='utf-8-sig')




