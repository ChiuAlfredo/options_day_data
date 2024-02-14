import pandas as pd 

def count_last_day_change(df_strike_price):    


    df_strike_price = df_strike_price.sort_index(ascending=True)

    # 獲取所需日期資料
    
    # 最後一天
    # 獲取最後一天的日期（不包括時間）
    last_day = df_strike_price.index.max().date()

    # 獲取最後一天的所有交易
    last_day_trades = df_strike_price.last('1D')

    # 獲取最後一筆交易
    last_last_trade = last_day_trades.iloc[-1]
    # 獲取第一筆交易
    last_first_trade = last_day_trades.iloc[0]

    # 計算最後一天變動%數
    last_day_change = (last_last_trade['MTF_PRICE'] - last_first_trade['MTF_PRICE']) / last_first_trade['MTF_PRICE']

    return last_day_change
   

def count_last_previos_day_change(df_strike_price):


    df_strike_price = df_strike_price.sort_index(ascending=True)

    # 獲取所需日期資料
    
    # 最後一天
    # 獲取最後一天的日期（不包括時間）
    last_day = df_strike_price.index.max().date()

    # 獲取最後一天的所有交易
    last_day_trades = df_strike_price.last('1D')

    # 獲取最後一筆交易
    last_last_trade = last_day_trades.iloc[-1]

    # 前一天
    # 獲取每天的日期
    date_day = df_strike_price.resample('1D').first().dropna().index.date

    if len(date_day) < 2:
        return None 

    # 獲取前一天的日期
    previous_day = date_day[-2]

    # 獲取前一天的所有交易
    previous_day_trades = df_strike_price[df_strike_price.index.date == previous_day]

    # 獲取前一天的最後一筆交易
    previous_last_trade = previous_day_trades.iloc[-1]


    # 計算最後-前一天變動%數
    last_previous_day_change = (last_last_trade['MTF_PRICE'] - previous_last_trade['MTF_PRICE']) / previous_last_trade['MTF_PRICE']        

    return last_previous_day_change