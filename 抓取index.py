import requests
import json
import pandas as pd 
from pymongo import MongoClient
from datetime import datetime, timedelta
import time 

client = MongoClient('mongodb://localhost:27017/')
db = client['option_price']
price_collection = db['index']



# 生成所有需要的日期
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 8, 7)
date_list = [(start_date + timedelta(days=x)).strftime('%Y%m%d') for x in range((end_date - start_date).days + 1)]

# 初始化空的 DataFrame
all_data = pd.DataFrame()

# 循環處理每個日期
for date in date_list:
    print(date)
    # date = date_list[2]
    url = f"https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_INDEX?date={date}&response=json&_=1723274024030"
    payload = {}
    headers = {
        'Cookie': 'JSESSIONID=5D9D4285744D8F7320FF3F2D6E048DB2'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    decoded_string = response.content.decode('utf-8')
    data = json.loads(decoded_string)

    if data['total'] != 0:
        df = pd.DataFrame(columns=data['fields'], data=data['data'])
        df['日期'] = date
        df['TimeStamp'] = pd.to_datetime(df['日期'] + df['時間'], format='%Y%m%d%H:%M:%S')
        df['發行量加權股價指數'] = df['發行量加權股價指數'].astype(str).str.replace(',', '').astype(float)
        
        filtered_df = df[((df['TimeStamp'].dt.time > pd.to_datetime('13:00').time()) & (df['TimeStamp'].dt.time <= pd.to_datetime('13:25').time())) |
                 (df['TimeStamp'].dt.time == pd.to_datetime('13:30').time())]
        filtered_df['平均值'] = filtered_df['發行量加權股價指數'].expanding().mean()
        
        hide_df =  df[((df['TimeStamp'].dt.time > pd.to_datetime('13:25').time()) & (df['TimeStamp'].dt.time < pd.to_datetime('13:30').time())) ]
        hide_df['平均值']=  filtered_df.loc[filtered_df['TimeStamp'].dt.time==pd.to_datetime('13:25:00').time(),'平均值'].values[0]
        
        df['平均值'] = df['發行量加權股價指數']

        df_no = df[df['TimeStamp'].dt.time <= pd.to_datetime('13:00').time() ]

        combined_df = pd.concat([df_no,filtered_df,hide_df]).drop_duplicates().reset_index(drop=True)

        combined_df = combined_df.sort_values(by='TimeStamp')

        
        combined_df['分鐘時間'] = combined_df['TimeStamp'].dt.ceil('T')
        
        combined_df = combined_df.sort_values(by='TimeStamp')

        # group_key = ('20240102', 'TXO', '15900', '202401', 'P')
        grouped_df = combined_df.groupby(['分鐘時間'])
        result = grouped_df.agg(
            開盤價=('平均值', 'first'),
            收盤價=('平均值', 'last'),
            最高價=('平均值', 'max'),
            最低價=('平均值', 'min'),
                ).reset_index()

        result_list = result.to_dict(orient='records')
        price_collection.insert_many(result_list)
    time.sleep(3)


