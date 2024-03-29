import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import plotly.graph_objects as go


def draw_daily_count_30min(df,file_folder):
    month = df['month'].iloc[0]
    kind_ = df['kind'].iloc[0]
    MTF_PROD_ID = df['MTF_PROD_ID'].iloc[0]
    strike_price = df['strike_price'].iloc[0]
    buy_sell = df['MTF_BS_CODE'].iloc[0]

    title = f'{month}_{kind_}_{strike_price}_{buy_sell}_{MTF_PROD_ID}_Half-Hourly-Price'

    df = df.sort_values('datetime')

   # 計算每30分鐘的MTF_PRICE * MTF_QNTY的總和
    df['price_qty'] = df['MTF_PRICE'] * df['MTF_QNTY']
    price_qty_sum = df.resample('30min', on='datetime')['price_qty'].sum()
    
    # 計算每30分鐘的MTF_QNTY的總和
    qty_sum = df.resample('30min', on='datetime')['MTF_QNTY'].sum()
    
    # 計算每30分鐘的價格
    half_hourly_price = price_qty_sum / qty_sum
    
    # 刪除含有NaN值的行
    half_hourly_price = half_hourly_price.dropna()


    # 獲取每天的日期
    date_day = half_hourly_price.resample('1D').first().dropna().index.date


    # 標記出每天的第一個交易時間
    date_list = []
    for date in date_day:
        date_list.append(half_hourly_price[half_hourly_price.index.date==date].index[0])

    # 每日的日期
    date_name_list = [date.strftime('%Y-%m-%d') for date in date_day]

     # 將日期轉換為字符串
    dates = half_hourly_price.index.strftime('%Y-%m-%d %H:%M:%S').unique()
    date_list = [date.strftime('%Y-%m-%d %H:%M:%S') for date in date_list]

    # 繪製圖表
    fig, ax = plt.subplots(figsize=(12, 12))

    # 繪製數據並設定 x 軸的標籤
    ax.plot(dates, half_hourly_price.values)
    ax.set_xticks(date_list,labels=date_name_list)
    # ax.set_xlabels(date_name_list, rotation=30)

    # 將x軸的標籤斜著顯示
    plt.xticks(rotation=90)
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.title(title)

    # Save the figure
    plt.savefig(f"./data/picture/{file_folder}/{title}.png")

    # #show
    # plt.show()
    plt.close()


def draw_prop_change(df_info,column,title,file_folder='prob'):
    expect_return = df_info[column].sum()/df_info[column].count()
    prob_list = []
    for i in range(0, 400, 20):
        mask = (df_info[column] >= i) 

        # Calculate the proportion
        proportion = round(mask.sum() / len(df_info),2)
        prob_list.append(proportion)

        # print(f">= {i} is {proportion}")

    # Create a bar plot
    fig = go.Figure(data=[go.Bar(x=list(range(0, 400, 20)), y=prob_list, text=prob_list, textposition='auto')])

    fig.update_layout(
        title=title+f',expect_return:{expect_return}',
        xaxis_title='Threshold',
        yaxis_title='Proportion',
        xaxis=dict(tickmode='linear', dtick=20),
    )

    # # Save the figure
    # fig.write_image(f"./data/picture/{file_folder}/{title}.png")

    return fig 
