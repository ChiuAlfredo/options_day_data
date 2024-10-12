import pymongo 
import pandas as pd
from datetime import datetime


# 連接 MongoDB
client = pymongo.MongoClient("mongodb://140.118.60.18:27017/")
db = client["option_price"]
collection = db["index"]


# 設定日期範圍
start_date = "2015-01-1T09:00:00Z"  # 替換為你的開始日期
end_date = "2024-08-07T13:30:00Z"  # 替換為你的結束日期

pipeline = [
    {
        # 過濾商品代號為 "TX" 的資料
        "$match": {
            "分鐘時間": {
                "$gte": pd.to_datetime(start_date),
                "$lte": pd.to_datetime(end_date)
            },
            "$expr": {
                "$or": [
                    {
                        "$and": [
                            {"$eq": [{"$hour": "$分鐘時間"}, 13]},  # 小时等于 9
                            {"$eq": [{"$minute": "$分鐘時間"}, 0]}  # 分钟等于 0
                        ]
                    },
                    {
                        "$and": [
                            {"$eq": [{"$hour": "$分鐘時間"}, 13]},  # 小时等于 13
                            {"$eq": [{"$minute": "$分鐘時間"}, 30]}  # 分钟等于 30
                        ]
                    }
                ]
            }
        }
    },
    {
        # 添加成交日期欄位
        "$addFields": {
            "成交日期": { "$dateToString": { "format": "%Y-%m-%d", "date": "$分鐘時間" } }
        }
    },
    {
        "$group": {
            "_id": "$成交日期",
            "openPrice": {
                "$last": {
                    "$cond": {
                        "if": {
                            "$and": [
                                {"$eq": [{"$hour": "$分鐘時間"}, 13]},  # 小时等于 9
                                {"$eq": [{"$minute": "$分鐘時間"}, 0]}  # 分钟等于 0
                            ]
                        },
                        "then": "$開盤價",
                        "else": None
                    }
                }
            },
            "closePrice": {
                "$first": {
                    "$cond": {
                        "if": {
                            "$and": [
                                {"$eq": [{"$hour": "$分鐘時間"}, 13]},  # 小时等于 13
                                {"$eq": [{"$minute": "$分鐘時間"}, 30]}  # 分钟等于 30
                            ]
                        },
                        "then": "$收盤價",
                        "else": None
                    }
                }
            }
        }
    },
    {
        "$project": {
            "成交日期": "$_id",
            "openPrice": 1,
            "closePrice": 1
        }
    },
    {
        "$sort": {"成交日期": 1}
    }
]

# 執行聚合查詢
result = collection.aggregate(pipeline)

# 轉換結果為 DataFrame
result_df = pd.DataFrame(list(result))




# result_df['20天平均'] = result_df['high_low_diff'].rolling(window=20).mean()
# result_df['20天平均_1230-1255'] = result_df['high_low_diff1230-1255'].rolling(window=20).mean()




df_end_date_info = pd.read_csv('結算日期.csv', encoding='utf-8',index_col=0)

# 將月份和日期轉換為兩位數格式
df_end_date_info['年'] = df_end_date_info['年'].astype(str)
df_end_date_info['月'] = df_end_date_info['月'].astype(str).str.zfill(2)
df_end_date_info['日'] = df_end_date_info['日'].astype(str).str.zfill(2)

df_end_date_info['結算日'] = pd.to_datetime(df_end_date_info['年'] + df_end_date_info['月'] + df_end_date_info['日'], format='%Y%m%d')

# Set the time part to 13:30:00
df_end_date_info['結算日'] = df_end_date_info['結算日'].apply(lambda x: x.replace(hour=13, minute=30, second=0))

df_end_date_info['結算日'] = df_end_date_info['結算日'].dt.strftime('%Y-%m-%d')


end_date_hl = pd.merge(result_df, df_end_date_info, left_on='成交日期', right_on='結算日', how='right')




# Convert time to minutes since 9:00 AM
def time_to_minutes(time_str):
    time_obj = pd.to_datetime(time_str, format='%H:%M')
    return time_obj.hour * 60 + time_obj.minute - 9 * 60



end_date_hl.to_excel('13-1330.xlsx', index=False)

