import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def draw_daily_count_30min(df,file_folder):
    month = df['month'].iloc[0]
    MTF_BS_CODE = df['MTF_BS_CODE'].iloc[0]
    MTF_PROD_ID = df['MTF_PROD_ID'].iloc[0]
    title = f'{month}_{MTF_BS_CODE}_{MTF_PROD_ID}_Half-Hourly-Price'


   # 計算每30分鐘的MTF_PRICE * MTF_QNTY的總和
    df.loc[:, 'price_qty'] = df['MTF_PRICE'] * df['MTF_QNTY']
    price_qty_sum = df.resample('30min', on='datetime')['price_qty'].sum()
    
    # 計算每30分鐘的MTF_QNTY的總和
    qty_sum = df.resample('30min', on='datetime')['MTF_QNTY'].sum()
    
    # 計算每30分鐘的價格
    half_hourly_price = price_qty_sum / qty_sum
    
    # 刪除含有NaN值的行
    half_hourly_price = half_hourly_price.dropna()

    fig, ax = plt.subplots(figsize=(12, 10))
    # 將日期轉換為字符串

    dates = half_hourly_price.index.strftime('%Y-%m-%d %H:%M:%S').unique()
    # 繪製數據並設定 x 軸的標籤

    ax.plot(dates, half_hourly_price.values)
    ax.xaxis.set_major_locator(plt.MaxNLocator(nbins=10))
    # 將x軸的標籤斜著顯示

    plt.xticks(rotation=30)
    plt.xlabel('Date')
    plt.ylabel('Price')
    
    plt.title(title)

    # Save the figure
    plt.savefig(f"./data/picture/{file_folder}/{title}.png")

    #show
    # plt.show()
