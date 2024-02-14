# %%
import pandas as pd
import os
import count_util

# 獲取 ./data/group 中的所有檔案名稱
file_folders = [os.path.splitext(file)[0] for file in os.listdir('./data/group') if file.endswith('.csv')]

df_info = pd.DataFrame(columns=['month', 'kind' , 'strike_price', 'MTF_BS_CODE','MTF_PROD_ID', 'last_day_change', 'previous_last_day_change'])

for file_folder in file_folders:
    # file_folder = file_folders[0]

    print(file_folder)

    month_data_df = pd.read_csv(f'data/group/{file_folder}.csv', encoding='utf-8-sig')
    month_data_df['datetime'] = pd.to_datetime(month_data_df['datetime'])
    month_data_df['MTF_DATE'] = pd.to_datetime(month_data_df['MTF_DATE'])

    # month3_data_df_16500 = month3_data_df[month3_data_df['strike_price']==16500]

    # 假設 df 是你的數據框，並且 'strike_price' 是其中的一個列
    for strike_price in month_data_df['strike_price'].unique():
        # strike_price = month_data_df['strike_price'].unique()[0]
        # strike_price  = 13500
        print(strike_price)
        

        # 獲取當前 strike_price 的數據
        df_strike_price = month_data_df[month_data_df['strike_price'] == strike_price]
        df_strike_price.set_index('datetime', inplace=True)
        df_strike_price.sort_index(ascending=True, inplace=True)

        # 先使用買價
        df_strike_price_B = df_strike_price[df_strike_price['MTF_BS_CODE']=='B']
        df_strike_price_S = df_strike_price[df_strike_price['MTF_BS_CODE']=='S']



        # 計算最後一天變動%數
        last_day_change_B = count_util.count_last_day_change(df_strike_price_B)
        last_day_change_S = count_util.count_last_day_change(df_strike_price_S)

        # 計算最後-前一天變動%數
        previous_last_day_change_B = count_util.count_last_previos_day_change(df_strike_price_B)
        previous_last_day_change_S = count_util.count_last_previos_day_change(df_strike_price_S)

        df_dict_B = {
            'month': df_strike_price_B['month'].iloc[0],
            'kind': df_strike_price_B['kind'].iloc[0],
            'strike_price': strike_price,
            'MTF_BS_CODE': 'B',
            'MTF_PROD_ID': df_strike_price['MTF_PROD_ID'].iloc[0],
            'last_day_change': last_day_change_B,
            'previous_last_day_change': previous_last_day_change_B
        }

        df_dict_S = {
            'month': df_strike_price_S['month'].iloc[0],
            'kind': df_strike_price_S['kind'].iloc[0],
            'strike_price': strike_price,
            'MTF_BS_CODE': 'S',
            'MTF_PROD_ID': df_strike_price['MTF_PROD_ID'].iloc[0],
            'last_day_change': last_day_change_S,
            'previous_last_day_change': previous_last_day_change_S
        }
        # 創建兩個新的 DataFrame，並將 df_dict_S 和 df_dict_B 的值分別添加到新的行
        new_row_S = pd.DataFrame(df_dict_S, index=[0])
        new_row_B = pd.DataFrame(df_dict_B, index=[0])

        # 將新的行添加到 df_info
        df_info = pd.concat([df_info, new_row_S,new_row_B])

df_info.reset_index(drop=True, inplace=True)
df_info.to_csv('df_info.csv', index=False, encoding='utf-8-sig')
        



#%%
# review 
df_info[df_info['last_day_change'] > 2]

                

