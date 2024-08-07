import pandas as pd


def count_last_day_change(df_strike_price,):    


    df_strike_price = df_strike_price.sort_index(ascending=True)

    # 獲取所需日期資料
    
    # 最後一天
    # 獲取最後一天的日期（不包括時間）
    last_day = df_strike_price.index.max().date()

    # 獲取最後一天的所有交易
    last_day_trades = df_strike_price.loc[df_strike_price.index.date == last_day]

    # 獲取最後一筆交易
    last_last_trade = last_day_trades.iloc[-1]['MTF_PRICE']
    # 獲取第一筆交易
    last_first_trade = last_day_trades.iloc[0]['MTF_PRICE']

    # # 計算最後一天變動%數
    # last_day_change = (last_last_trade['MTF_PRICE'] - last_first_trade['MTF_PRICE']) / last_first_trade['MTF_PRICE']

    # 前一天
    # 獲取每天的日期
    date_day = df_strike_price.resample('1D').first().dropna().index.date

    if len(date_day) >= 2:
        # 獲取前一天的日期
        previous_day = date_day[-2]

        # 獲取前一天的所有交易
        previous_day_trades = df_strike_price[df_strike_price.index.date == previous_day]

        # 獲取前一天的最後一筆交易
        previous_last_trade = previous_day_trades.iloc[-1]['MTF_PRICE']
    else:
        previous_day = None
        previous_last_trade = None

    return previous_last_trade,last_first_trade,last_last_trade,last_day,previous_day
   
def day_deal_count_last_day_change(df,):
    df = df.sort_index(ascending=True)

    # 獲取所需日期資料
    
    # 最後一天
    # 獲取最後一天的日期（不包括時間）
    last_day = df.index.max().date()

    # 獲取最後一天的所有交易
    last_day_trades = df.loc[df.index.date == last_day]


    # # 計算最後一天變動%數
    # last_day_change = (last_last_trade['MTF_PRICE'] - last_first_trade['MTF_PRICE']) / last_first_trade['MTF_PRICE']

    # 前一天
    # 獲取每天的日期
    date_day = df.resample('1D').first().dropna().index.date

    if len(date_day) >= 2:
        # 獲取前一天的日期
        previous_day = date_day[-2]

        # 獲取前一天的所有交易
        previous_day_trades = df[df.index.date == previous_day]

    else:
        previous_day = None
        previous_last_trade = None
        previous_end_trade = None

    return last_day_trades,previous_day_trades,last_day,previous_day
   