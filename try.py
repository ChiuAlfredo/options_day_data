import pymongo 
import pandas as pd
from datetime import datetime

# 連接 MongoDB
client = pymongo.MongoClient("mongodb://140.118.60.18:27017/")
db = client["option_price"]
collection = db["index"]


# 設定日期範圍
start_date = "2022-01-1T08:45:00Z"  # 替換為你的開始日期
end_date = "2024-08-07T13:30:00Z"  # 替換為你的結束日期


# 聚合管道
pipeline = [
    {
        # 過濾商品代號為 "TX" 的資料
        "$match": {
            "商品代號": "TX",
            "時間": {
                "$gte": pd.to_datetime(start_date),
                "$lte": pd.to_datetime(end_date)
            },
            "$expr": {
                "$or": [
                    {
                        "$and": [
                            {"$eq": [{"$hour": "$時間"}, 13]},  # 小时等于 9
                            {"$eq": [{"$minute": "$時間"}, 0]}  # 分钟等于 0
                        ]
                    },
                    {
                        "$and": [
                            {"$eq": [{"$hour": "$時間"}, 13]},  # 小时等于 13
                            {"$eq": [{"$minute": "$時間"}, 30]}  # 分钟等于 30
                        ]
                    }
                ]
            }
        }
    },
    {
        # 添加成交日期欄位
        "$addFields": {
            "成交日期": { "$dateToString": { "format": "%Y-%m-%d", "date": "$時間" } }
        }
    },
    {
        # 按成交日期分組，計算每個日期的最小到期月份(週別)
        "$group": {
            "_id": "$成交日期",
            "min_expiry": { "$min": "$到期月份(週別)" },
            "records": { "$push": "$$ROOT" }  # 保存所有原始記錄
        }
    },
    {
        # 展開 records 陣列
        "$unwind": "$records"
    },
    {
        # 過濾出到期月份(週別)等於最小值的記錄
        "$match": {
            "$expr": { "$eq": ["$records.到期月份(週別)", "$min_expiry"] }
        }
    },
    {
        # 選擇需要的欄位
        "$replaceRoot": { "newRoot": "$records" }
    },
    {
        # 根據時間排序
        "$sort": { "時間": 1 }
    },
    {
        "$group": {
            "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$時間"}},
            "openPrice": {
                "$first": {
                    "$cond": {
                        "if": {
                            "$and": [
                                {"$eq": [{"$hour": "$時間"}, 9]},  # 小时等于 9
                                {"$eq": [{"$minute": "$時間"}, 0]}  # 分钟等于 0
                            ]
                        },
                        "then": "$開盤價",
                        "else": None
                    }
                }
            },
            "closePrice": {
                "$last": {
                    "$cond": {
                        "if": {
                            "$and": [
                                {"$eq": [{"$hour": "$時間"}, 13]},  # 小时等于 13
                                {"$eq": [{"$minute": "$時間"}, 30]}  # 分钟等于 30
                            ]
                        },
                        "then": "$收盤價",
                        "else": None
                    }
                }
            },
            "records": { "$push": "$$ROOT" }  # 保存所有原始記錄
        }
    },
    {
        "$project": {
            "成交日期": "$_id",
            "openPrice": 1,
            "closePrice": 1,
            "openPriceTime": {
                "$arrayElemAt": [
                    {
                        "$filter": {
                            "input": "$records",
                            "as": "item",
                            "cond": {
                                "$and": [
                                    {"$eq": ["$$item.開盤價", "$openPrice"]},
                                    {"$eq": [{"$hour": "$$item.時間"}, 9]},  # 小时等于 9
                                    {"$eq": [{"$minute": "$$item.時間"}, 0]}  # 分钟等于 0
                                ]
                            }
                        }
                    },
                    0
                ]
            },
            "closePriceTime": {
                "$arrayElemAt": [
                    {
                        "$filter": {
                            "input": "$records",
                            "as": "item",
                            "cond": {
                                "$and": [
                                    {"$eq": ["$$item.收盤價", "$closePrice"]},
                                    {"$eq": [{"$hour": "$$item.時間"}, 13]},  # 小时等于 13
                                    {"$eq": [{"$minute": "$$item.時間"}, 30]}  # 分钟等于 30
                                ]
                            }
                        }
                    },
                    0
                ]
            }
        }
    },
    {
        "$project": {
            "成交日期": 1,
            "openPrice": 1,
            "closePrice": 1,
            "openPriceTime": "$openPriceTime.時間",
            "closePriceTime": "$closePriceTime.時間"
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




end_date_hl.to_csv("result.csv", index=False)  # 將結果保存為 CSV 檔案