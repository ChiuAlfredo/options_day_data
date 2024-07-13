import os
from util.BS_util import BS_formula
import pandas as pd
from functools import reduce


def read_index(year, month, day, kind):
    df_index = pd.read_csv(
        f'./data/指數/{year}{month}{day}_{kind}.csv', encoding='utf-8-sig'
    )
    df_index['發行量加權股價指數'] = (
        df_index['發行量加權股價指數']
        .replace(',', '', regex=True)
        .astype(float)
    )

    # 將 df_index['時間'] 的日期部分替換為最大日期
    df_index['時間'] = str(year) + ":" + str(month) + ":" + str(day) + ":" + df_index['時間']
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
    df_index_new = df_index.iloc[::12]
    df_index_new = df_index_new[:-1]
    
    df_index_new = pd.concat([df_index_new,df_index.iloc[-12:]])

    df_index_new = df_index_new.iloc[:-12]
    

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

def read_future_price(file_path):
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
        ).round(4)

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
        ).round(4)

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
                    # 使用上一個時間點的成交價，假設df_price已經按時間排序
                if t > 0:  # 確保不是第一行
                    df_price.at[t, f'{k}_成交價'] = df_price.at[t-1, f'{k}_成交價']
                else:
                    df_price.at[t, f'{k}_成交價'] = 0  # 如果是第一行，沒有上一個時間點，設為0或其他預設值
            else:
                price = (df_t['MTF_PRICE']*df_t['MTF_QNTY']).sum()/df_t['MTF_QNTY'].sum()
                quantity = df_t['MTF_QNTY'].sum()

                df_price.at[t, f'{k}_成交量'] = quantity.round(3)
                df_price.at[t, f'{k}_成交價'] = price.round(3)


            
    return df_price

def get_future_price(month_future_price_df):

    df_price =pd.DataFrame()
    df_price['時間']= month_index['時間']

        # k = strike_price_list[0]


    df_price[f'期貨_成交量'] = 0
    df_price[f'期貨_成交價'] = 0

    df_price[f'期貨_成交量'].astype(float)
    df_price[f'期貨_成交價'].astype(float)
    for t in range(len(df_price)):
        # print(t)
        # t = 24
        if t ==0:
            df_t = month_future_price_df[month_future_price_df['datetime']<= df_price['時間'][t]]
        
        else:
            df_t = month_future_price_df[(month_future_price_df['datetime']<= df_price['時間'][t])&(month_future_price_df['datetime']> df_price['時間'][t-1])]
        if df_t.empty:
            df_price.at[t, f'期貨_成交量'] = 0
                # 使用上一個時間點的成交價，假設df_price已經按時間排序
            if t > 0:  # 確保不是第一行
                df_price.at[t, f'期貨_成交價'] = df_price.at[t-1, f'期貨_成交價']
            else:
                df_price.at[t, f'期貨_成交價'] = 0  # 如果是第一行，沒有上一個時間點，設為0或其他預設值
        else:
            price = (df_t['MTF_PRICE']*df_t['MTF_QNTY']).sum()/df_t['MTF_QNTY'].sum()
            quantity = df_t['MTF_QNTY'].sum()

            df_price.at[t, f'期貨_成交量'] = quantity.round(3)
            df_price.at[t, f'期貨_成交價'] = price.round(3)


            
    return df_price


df_end_date_info = pd.read_excel('結算日日期和種類.xlsx')

# 將月份和日期轉換為兩位數格式
df_end_date_info['月'] = df_end_date_info['月'].astype(str).str.zfill(2)
df_end_date_info['日'] = df_end_date_info['日'].astype(str).str.zfill(2)

for index, row in df_end_date_info.iterrows():
    # row = df_end_date_info.iloc[0]

    year = row['年']
    month = row['月']
    day = row['日']
    kind = row['種類']


    month_price_df = read_option_price(f'data/選擇權結算/{year}_{month}_{day}_{kind}.gzip') 

    month_future_price_df = read_future_price(f'data/期貨結算/{year}_{month}_{day}.gzip')
    

    month_price_c = month_price_df[month_price_df['kind']=='C']
    month_price_p = month_price_df[month_price_df['kind']=='P']
    month_index = read_index(year, month, day, kind)[1]
    month_index['發行量加權股價指數_變化'] = month_index['發行量加權股價指數'].diff().round(2)


    df_future_price = get_future_price(month_future_price_df).set_index('時間')
    df_future_price['期貨_成交價_變化'] = df_future_price['期貨_成交價'].diff().round(2)



    price_range = {
        'min': round(month_index['發行量加權股價指數'].mean() / 100) * 100 - 500,
        'max': round(month_index['發行量加權股價指數'].mean() / 100) * 100 + 500,
        'step': 100,
    }

    strike_price_list = sorted(month_price_c[
            (month_price_c['strike_price'] < price_range['max'])
            & (month_price_c['strike_price'] > price_range['min'])
        ]['strike_price'].unique().tolist())

    df_price_c = get_option_price(month_price_c,strike_price_list).set_index('時間')
    df_price_p = get_option_price(month_price_p,strike_price_list).set_index('時間')
    


    df_price_c.columns = [f"C_{col}" for col in df_price_c.columns]
    df_price_p.columns = [f"P_{col}" for col in df_price_p.columns]



    df_delta = get_delta(month_index,strike_price_list)
    df_gamma = get_gamma(month_index,strike_price_list)
    df_bs_price_c,df_bs_price_p = get_bs_price_c(month_index,strike_price_list)

    def get_real_delta_gamma(month_index,df_price,strike_price_list,kind='C'):
        df_delta = pd.DataFrame()
        df_gamma = pd.DataFrame()
        # 遍歷所有strike prices
        for strike_price in strike_price_list:
            # strike_price = strike_price_list[0]
            # 計算df_price中特定strike price的成交量變化
            df_price[f'{kind}_{strike_price}_成交價_變化'] = df_price[f'{kind}_{strike_price}_成交價'].diff().round(2)

            # 計算delta
            df_delta[f'{kind}_{strike_price}_real_delta'] = (df_price[f'{kind}_{strike_price}_成交價_變化'] / month_index['發行量加權股價指數_變化'].values).round(4)

            # 計算gamma
            df_gamma[f'{kind}_{strike_price}_real_gamma'] = (df_delta[f'{kind}_{strike_price}_real_delta'].diff().round(2) / month_index['發行量加權股價指數_變化'].values).round(4)


        return df_delta,df_gamma
    # real delta gamma
    df_real_delta_c,df_real_gamma_c = get_real_delta_gamma(month_index,df_price = df_price_c,strike_price_list = strike_price_list,kind='C')
    df_real_delta_p,df_real_gamma_p = get_real_delta_gamma(month_index,df_price_p,strike_price_list,kind='P')




# month_index


    # List of dataframes to merge
    dfs = [df_delta, df_gamma, df_bs_price_c, df_bs_price_p, df_price_c, df_price_p, month_index,df_future_price,df_real_delta_c,df_real_gamma_c,df_real_delta_p,df_real_gamma_p]

    # Use reduce to merge all dataframes
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


    destanation_dir = './data/每一分鐘資料/'
    target_dir = build_folder('',destanation_dir)

    month_data_df_5min = reduce(lambda left,right: pd.merge(left,right,on='時間', how='outer'), dfs)
    month_data_df_5min.to_csv(f'./{destanation_dir}{year}_{month}_{day}_{kind}每一分鐘資料.csv',index=False,encoding='utf-8-sig')




