import pymongo 
import pandas as pd
from datetime import datetime

# 連接 MongoDB
client = pymongo.MongoClient("mongodb://140.118.60.18:27017/")
db = client["option_price"]
collection = db["future"]



# 設定日期範圍
start_date = "2011-01-1T08:45:00Z"  # 替換為你的開始日期
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
                "$and": [
                    {"$gte": [{"$hour": "$時間"}, 9]},  # 时间大于等于 9:00
                    {
                        "$or": [
                            {"$lt": [{"$hour": "$時間"}, 13]},  # 时间在13点前
                            {
                                "$and": [
                                    {"$eq": [{"$hour": "$時間"}, 13]},  # 如果是13点
                                    {"$lte": [{"$minute": "$時間"}, 30]}  # 分钟数小于等于30
                                ]
                            }
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
            "highestPrice": {"$max": "$最高價"},
            "lowestPrice": {"$min": "$最低價"},
            "records": { "$push": "$$ROOT" }  # 保存所有原始記錄
        }
    },
    {
        "$project": {
            "成交日期": "$_id",
            "highestPrice": 1,
            "lowestPrice": 1,
            "high_low_diff": {"$subtract": ["$highestPrice", "$lowestPrice"]},
            "highestPriceTime": {
                "$arrayElemAt": [
                    {
                        "$filter": {
                            "input": "$records",
                            "as": "item",
                            "cond": {"$eq": ["$$item.最高價", "$highestPrice"]}
                        }
                    },
                    0
                ]
            },
            "lowestPriceTime": {
                "$arrayElemAt": [
                    {
                        "$filter": {
                            "input": "$records",
                            "as": "item",
                            "cond": {"$eq": ["$$item.最低價", "$lowestPrice"]}
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
            "highestPrice": 1,
            "lowestPrice": 1,
            "high_low_diff": 1,
            "highestPriceTime": "$highestPriceTime.時間",
            "lowestPriceTime": "$lowestPriceTime.時間"
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

# 顯示結果
print(result_df)

result_df['20天平均'] = result_df['high_low_diff'].rolling(window=20).mean()




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



end_date_hl['highestPriceTime_cal'] = [time_to_minutes(t) for t in end_date_hl['highestPriceTime']]
end_date_hl['lowestPriceTime_cal'] = [time_to_minutes(t) for t in end_date_hl['lowestPriceTime']]
end_date_hl['時間差'] = end_date_hl['highestPriceTime_cal'] - end_date_hl['lowestPriceTime_cal']


probability = (end_date_hl['時間差'].abs() > 180).sum()/ len(end_date_hl)

print(f"時間差的絕對值大於 180 分鐘的機率: {probability:.2%}")

end_date_hl.to_csv('20天平均.csv', encoding='utf-8-sig', index=False)

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
                "$and": [
                    {"$gte": [{"$hour": "$時間"}, 9]},  # 时间大于等于 9:00
                    {
                        "$or": [
                            {"$lt": [{"$hour": "$時間"}, 13]},  # 时间在13点前
                            {
                                "$and": [
                                    {"$eq": [{"$hour": "$時間"}, 13]},  # 如果是13点
                                    {"$lte": [{"$minute": "$時間"}, 30]}  # 分钟数小于等于30
                                ]
                            }
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
        "highestPrice": {"$max": "$最高價"},
        "lowestPrice": {"$min": "$最低價"},
        "highest_in_9_to_10": {
            "$max": {
                "$cond": {
                    "if": {
                        "$and": [
                            {"$gte": [{"$hour": "$時間"}, 9]},
                            {"$lt": [{"$hour": "$時間"}, 10]}
                        ]
                    },
                    "then": "$最高價",
                    "else": None
                }
            }
        },
        "lowest_in_9_to_10": {
            "$min": {
                "$cond": {
                    "if": {
                        "$and": [
                            {"$gte": [{"$hour": "$時間"}, 9]},
                            {"$lt": [{"$hour": "$時間"}, 10]}
                        ]
                    },
                    "then": "$最低價",
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
        "highestPrice": 1,
        "lowestPrice": 1,
        "high_low_diff": {"$subtract": ["$highestPrice", "$lowestPrice"]},
        "highestPriceTime": {
            "$arrayElemAt": [
                {
                    "$filter": {
                        "input": "$records",
                        "as": "item",
                        "cond": {"$eq": ["$$item.最高價", "$highestPrice"]}
                    }
                },
                0
            ]
        },
        "lowestPriceTime": {
            "$arrayElemAt": [
                {
                    "$filter": {
                        "input": "$records",
                        "as": "item",
                        "cond": {"$eq": ["$$item.最低價", "$lowestPrice"]}
                    }
                },
                0
            ]
        },
        "highest_in_9_to_10": 1,
        "highest_in_9_to_10_time": {
            "$arrayElemAt": [
                {
                    "$filter": {
                        "input": "$records",
                        "as": "item",
                        "cond": {
                            "$and": [
                                {"$eq": ["$$item.最高價", "$highest_in_9_to_10"]},
                                {"$gte": [{"$hour": "$$item.時間"}, 9]},
                                {"$lt": [{"$hour": "$$item.時間"}, 10]}
                            ]
                        }
                    }
                },
                0
            ]
        },
        "lowest_in_9_to_10": 1,
        "lowest_in_9_to_10_time": {
            "$arrayElemAt": [
                {
                    "$filter": {
                        "input": "$records",
                        "as": "item",
                        "cond": {
                            "$and": [
                                {"$eq": ["$$item.最低價", "$lowest_in_9_to_10"]},
                                {"$gte": [{"$hour": "$$item.時間"}, 9]},
                                {"$lt": [{"$hour": "$$item.時間"}, 10]}
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
        "highestPrice": 1,
        "lowestPrice": 1,
        "high_low_diff": 1,
        "highestPriceTime": "$highestPriceTime.時間",
        "lowestPriceTime": "$lowestPriceTime.時間",
        "highest_in_9_to_10": 1,
        "highest_in_9_to_10_time": "$highest_in_9_to_10_time.時間",
        "lowest_in_9_to_10": 1,
        "lowest_in_9_to_10_time": "$lowest_in_9_to_10_time.時間"
    }
},
{
    "$sort": {"成交日期": 1}
}
]