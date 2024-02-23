# %%
import pandas as pd
import os
import util.count_util as count_util

# 獲取 ./data/group 中的所有檔案名稱
file_folders = [os.path.splitext(file)[0] for file in os.listdir('./data/group') if file.endswith('.csv')]

df_info = pd.DataFrame(columns=['month', 'kind' , 'strike_price', 'MTF_BS_CODE','MTF_PROD_ID', 'last_day_change', 'previous_last_day_change'])

for file_folder in file_folders:
    # file_folder = file_folders[0]

    print(file_folder)

    month_data_df = pd.read_parquet(f'data/group/{file_folder}.gzip')
    month_data_df['datetime'] = pd.to_datetime(month_data_df['datetime'])
    month_data_df['MTF_DATE'] = pd.to_datetime(month_data_df['MTF_DATE'])

    # month3_data_df_16500 = month3_data_df[month3_data_df['strike_price']==16500]

    # 假設 df 是你的數據框，並且 'strike_price' 是其中的一個列
    for strike_price in month_data_df['strike_price'].unique():
        # strike_price = month_data_df['strike_price'].unique()[0]
        # strike_price  = 13500
        # print(strike_price)
        

        # 獲取當前 strike_price 的數據
        df_strike_price = month_data_df[month_data_df['strike_price'] == strike_price]
        df_strike_price.set_index('datetime', inplace=True)

        # 先使用買價
        df_strike_price_B = df_strike_price[df_strike_price['MTF_BS_CODE']=='B']
        df_strike_price_S = df_strike_price[df_strike_price['MTF_BS_CODE']=='S']



        # 計算最後一天變動%數
        for BS_CODE in ['B', 'S']:
            # BS_CODE = 'B'
            df_strike_price_BS = df_strike_price[df_strike_price['MTF_BS_CODE']==BS_CODE]
            previous_last_trade,last_first_trade,last_last_trade = count_util.count_last_day_change(df_strike_price_BS)

            df_dict = {
                'month': df_strike_price_B['month'].iloc[0],
                'kind': df_strike_price_B['kind'].iloc[0],
                'strike_price': strike_price,
                'MTF_BS_CODE': BS_CODE,
                'MTF_PROD_ID': df_strike_price['MTF_PROD_ID'].iloc[0],
                'previous_last_price': previous_last_trade,
                'last_first_price': last_first_trade,
                'last_last_price':last_last_trade
            }

            # 創建兩個新的 DataFrame，並將 df_dict_S 和 df_dict_B 的值分別添加到新的行
            new_row_df = pd.DataFrame(df_dict, index=[0])

            # 將新的行添加到 df_info
            df_info = pd.concat([df_info, new_row_df])

df_info.reset_index(drop=True, inplace=True)

# 計算last_first-last變動%
df_info['last_day_change'] = (df_info['last_last_price'] - df_info['last_first_price']) / df_info['last_first_price'] * 100

# 計算previous_last_first變動%
df_info['previous_last_day_change'] = (df_info['last_first_price'] - df_info['previous_last_price']) / df_info['previous_last_price'] * 100

df_info.sort_values(by=['month', 'kind', 'strike_price'], inplace=True)
df_info.to_csv('df_info.csv', index=False, encoding='utf-8-sig')
#%%
# review 
TX_df = pd.read_csv('TX2-4.csv',encoding='utf-8',sep='\t')

TX_df['datetime'] = pd.to_datetime(TX_df['年月日'], format='%Y%m%d')
TX_df.set_index('datetime', inplace=True)
TX_group = TX_df.groupby('證券代碼')

new_TX_df = pd.DataFrame()

for name, group in TX_group:

    # 列出所有日期
    date_day = group.resample('1D').first().dropna().index.date
    
    # 倒數一天
    last_day = date_day[-1]

    # 倒數第二天
    previous_day = date_day[-2]

    # 倒數第三天
    previous_2_day = date_day[-3]

    # 選擇最後三天的數據
    new_TX_selected = group[group.index.isin([last_day, previous_day, previous_2_day])]

    # 合併數據
    new_TX_df = pd.concat([new_TX_df, new_TX_selected])

new_TX_df.to_csv('TX_last3day.csv', index=False, encoding='utf-8-sig')


#%% 
import matplotlib.pyplot as plt
prob_list = []
for i in range(0,200,20):
    mask = df_info['previous_last_day_change'] >= i

    # Calculate the proportion
    proportion = mask.sum() / len(df_info)
    prob_list.append(proportion)

    print(f">= {i} is {proportion}")

# Create a bar plot
plt.figure(figsize=(10, 6))
bars = plt.bar(range(0, 200, 20), prob_list)

plt.xlabel('Threshold')
plt.ylabel('Proportion')
plt.title('Proportion of Rows where \'previous_last_day_change\' is >= Threshold')
plt.xticks(range(0, 200, 20))
# Add data labels
for bar, proportion in zip(bars, prob_list):
    plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01, f'{proportion:.2f}', ha='center', va='bottom')

plt.show()




