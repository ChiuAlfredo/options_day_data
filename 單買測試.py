import os
from datetime import timedelta
import pandas as pd
import warnings

from data_util import read_option_price, read_future_price, read_index, buil_Data, read_end_date
from option_action import Option

# Ignore warnings for cleaner output
pd.options.mode.chained_assignment = None
pd.options.mode.use_inf_as_na = True
warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

end_date_df = read_end_date()

# Parameters
move_stop_loss = 0.5
fix_stop_loss = 0.1
moneyness = 1
initial_cash = 1000
global profit_total
profit_total = 0
CorP = 'C'  # Call options only
hedge_frequency = 10  # No longer relevant
record_list = []

# Helper functions
def find_most_itm_option(future_price):
    itm_strike_price = future_price - future_price % 50
    return itm_strike_price

def filter_merged_df(merged_df, **kwargs):
    """
    Filter merged_df based on conditions provided in kwargs.
    """
    condition = pd.Series([True] * len(merged_df))
    for key, value in kwargs.items():
        condition &= (merged_df[key] == value)
    return merged_df.loc[condition]

# Main trading loop
for index, row in end_date_df.iterrows():
    if index == 0:
        continue
    elif index > 5:  # Limit for testing, adjust as needed
        break
    else:
        start_date = end_date_df['結算日'][index - 1] - timedelta(hours=4, minutes=45)
        end_date = end_date_df['結算日'][index - 1]
        week_kind = end_date_df['到期月份(週別)'][index - 1]

        # Load data
        month_option_price_df = read_option_price(start_date, end_date, week_kind)
        month_future_price_df_raw = read_future_price(start_date, end_date)
        month_index = read_index(start_date, end_date)
        
        merged_df = buil_Data(month_option_price_df, month_future_price_df_raw, month_index, end_date_df)
        merged_df.sort_values(by='時間', inplace=True)

        # Initialize trading
        n = 3  # Number of trades allowed
        option = Option()
        
        for future_index, future_row in month_future_price_df_raw.iterrows():
        # Buy call option at 9:00
            if (future_row['時間'].strftime("%Y-%m-%d") == end_date.strftime("%Y-%m-%d")) & (future_row['時間'].hour >= 9):
                 if (future_row['時間'].hour == 9) & (future_row['時間'].minute >= 0) & (option.position_list['option'] == []) & (n > 0):
                     choose_strike = find_most_itm_option(future_price=future_row['收盤價']) + moneyness * 50
                     option_row = filter_merged_df(merged_df, 時間=future_row['時間'], 履約價格=choose_strike, 買賣權別=CorP)
                     if option_row.empty:
                         continue

                     price = option_row['選擇權_收盤價'].values[0]
                    # Modify to correctly use 'B' (Buy) for buying
                     option.option_trade(future_row['時間'], 'B', CorP, choose_strike, price, 1, 0, position_action='open')
                     n -= 1

             # Close position at 13:30
            if (future_row['時間'].hour == 13) & (future_row['時間'].minute == 30):
                if option.position_list['option'] != []:
                     for position in option.position_list['option']:
                        # Modify to correctly use 'S' (Sell) for selling
                         option.option_trade(future_row['時間'], 'S', CorP, position['履約價'], position['現價'], 1, 0, position_action='close')
        # Record trades
        record_dict = {
            '日期': start_date.strftime('%Y-%m-%d'),
            '加權指數': month_index,
            '期貨': month_future_price_df_raw,
            '選擇權紀錄': option.record_list['option'],
        }
        record_list.append(record_dict)

# Calculate profit statistics
profit_list = []
for record in record_list:
    profit = 0
    if record['選擇權紀錄'] != []:
        profit += record['選擇權紀錄'][-1]['損益累計']
    profit_list.append(float(profit))

# Statistics summary
statistic_data_dict = {
    '獲利次數': sum(1 for profit in profit_list if profit > 0),
    '虧損次數': sum(1 for profit in profit_list if profit <= 0),
    '獲利金額': sum(profit for profit in profit_list if profit > 0),
    '虧損金額': sum(profit for profit in profit_list if profit <= 0),
}

statistic_dict = {
    '期間': f'{record_list[0]["日期"]}~{record_list[-1]["日期"]}',
    '總淨利': statistic_data_dict['獲利金額'] + statistic_data_dict['虧損金額'],
    '毛利': statistic_data_dict['獲利金額'],
    '毛損失': statistic_data_dict['虧損金額'],
    '總交易天數': len(profit_list),
    '勝率': statistic_data_dict['獲利次數'] / (statistic_data_dict['獲利次數'] + statistic_data_dict['虧損次數']) if (statistic_data_dict['獲利次數'] + statistic_data_dict['虧損次數']) > 0 else 0,
    '成功筆數': statistic_data_dict['獲利次數'],
    '失敗筆數': statistic_data_dict['虧損次數'],
    '最大獲利交易': max(profit_list) if profit_list else 0,
    '最大虧損交易': min(profit_list) if profit_list else 0,
    '成功交易平均獲利': statistic_data_dict['獲利金額'] / statistic_data_dict['獲利次數'] if statistic_data_dict['獲利次數'] > 0 else 0,
    '失敗交易平均虧損': statistic_data_dict['虧損金額'] / statistic_data_dict['虧損次數'] if statistic_data_dict['虧損次數'] > 0 else 0,
    '平均獲利/平均虧損': (statistic_data_dict['獲利金額'] / statistic_data_dict['獲利次數']) / (statistic_data_dict['虧損金額'] / statistic_data_dict['虧損次數']) if statistic_data_dict['獲利次數'] > 0 and statistic_data_dict['虧損次數'] > 0 else 0,
    '平均每筆交易盈虧': (statistic_data_dict['獲利金額'] + statistic_data_dict['虧損金額']) / (statistic_data_dict['獲利次數'] + statistic_data_dict['虧損次數']) if (statistic_data_dict['獲利次數'] + statistic_data_dict['虧損次數']) > 0 else 0,
}

# Save records and statistics
with pd.ExcelWriter(f'record_list_{record_list[0]["日期"]}-{record_list[-1]["日期"]}.xlsx') as writer:
    statistic_df = pd.DataFrame(statistic_dict, index=[0])
    statistic_df.transpose().to_excel(writer, sheet_name='統計', header=False)
    for record in record_list:
        df = pd.DataFrame()
        
        if record['選擇權紀錄'] == []:
            df_option = pd.DataFrame(columns=['時間_選擇權紀錄','BorS_選擇權紀錄','倉位狀況_選擇權紀錄','履約價_選擇權紀錄','CorP_選擇權紀錄','價格_選擇權紀錄','數量_選擇權紀錄','手續費_選擇權紀錄','損益_選擇權紀錄','損益累計_選擇權紀錄'])
        else:
            df_option = pd.DataFrame(record['選擇權紀錄']).add_suffix('_選擇權紀錄')
            
        df = pd.merge(record['加權指數'], record['期貨'], left_on='分鐘時間', right_on='時間', how='inner', suffixes=('_加權指數', '_期貨'))
        df = df[['時間', '收盤價_加權指數', '收盤價_期貨']]
        df = pd.merge(df, df_option, left_on='時間', right_on='時間_選擇權紀錄', how='left', suffixes=('', '_選擇權紀錄'))
        df.sort_values(by='時間', inplace=True)
        
        sheet_name = record['日期']
        df.to_excel(writer, sheet_name=sheet_name, index=False)

print("record_list 已保存為 record_list.xlsx")
