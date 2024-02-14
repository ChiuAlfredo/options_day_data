# %%
import pandas as pd
import os
import draw_util


# 獲取 ./data/group 中的所有檔案名稱
file_folders = [os.path.splitext(file)[0] for file in os.listdir('./data/group') if file.endswith('.csv')]

for file_folder in file_folders:
    # file_folder = file_folders[0]

    print(file_folder)

    # 檢查 file_folder 是否存在於 ./data/picture/
    if not os.path.exists(f'./data/picture/{file_folder}'):
        # 建立新的資料夾
        os.makedirs(f'./data/picture/{file_folder}', exist_ok=True)


    month_data_df = pd.read_csv(f'data/group/{file_folder}.csv', encoding='utf-8-sig')
    month_data_df['datetime'] = pd.to_datetime(month_data_df['datetime'])
    month_data_df['MTF_DATE'] = pd.to_datetime(month_data_df['MTF_DATE'])


    # month3_data_df_16500 = month3_data_df[month3_data_df['strike_price']==16500]

    # 假設 df 是你的數據框，並且 'strike_price' 是其中的一個列
    for strike_price in month_data_df['strike_price'].unique():

        # strike_price = month_data_df['strike_price'].unique()[0]
        # 獲取當前 strike_price 的數據
        df_strike_price = month_data_df[month_data_df['strike_price'] == strike_price]

        df_strike_price_B = df_strike_price[df_strike_price['MTF_BS_CODE']=='B']
        df_strike_price_S = df_strike_price[df_strike_price['MTF_BS_CODE']=='S']
        # 繪製圖表
        draw_util.draw_daily_count_30min(df_strike_price_B,file_folder)
        draw_util.draw_daily_count_30min(df_strike_price_S,file_folder)






