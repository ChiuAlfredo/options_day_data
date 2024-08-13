import time
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pymongo
import streamlit as st
from plotly.subplots import make_subplots

# 配置 MongoDB 连接
client = pymongo.MongoClient('localhost', 27017)
db = client["option_price"]
collection = db["minute_summary"]

collection_future_info = db["future_info"]
collection_option_info = db["option_info"]

# Streamlit 应用
st.title("Real-Time Stock Visualization")

# 查询 MongoDB 数据
@st.cache_data(ttl=60)
def get_data(query):
    cursor = collection.find(query)
    data = list(cursor)
    df = pd.DataFrame(data)
    return df

def organize_data(df):

    # 设置 TimeStamp 列为索引
    df.set_index('TimeStamp', inplace=True)
    
    # 筛选时间范围在 08:45 到 13:30 之间的数据
    df_filtered = df.between_time('08:45', '13:30')
    
    # 重置索引
    df_filtered.reset_index(inplace=True)

    df_filtered.sort_values(by='TimeStamp', inplace=True)

    df_filtered['volume'] = df_filtered['volume'].astype(float)
    df_filtered['price'] = df_filtered['price'].astype(float)
    

    return df_filtered

def draw_plot(df_option, query):
    # 創建一個包含兩個子圖的圖表
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # 添加成交價的折線圖
    fig.add_trace(
        go.Scatter(x=df_option['TimeStamp'], y=df_option['price'], name='成交價', mode='lines'),
        secondary_y=False,
    )
    # 设置 y 轴为对数刻度
    fig.update_layout(
        yaxis_type="log",
        yaxis_title="成交價",
        title="成交價折線圖 (對數刻度)"
    )

    # 添加成交量的長條圖
    fig.add_trace(
        go.Bar(x=df_option['TimeStamp'], y=df_option['volume'], name='成交量', opacity=0.6),
        secondary_y=True,
    )


    # 設置圖表標題和軸標籤
    fig.update_layout(
        title_text=f'Price and Volume Trend for {query["COMMODITYID"]}',
        xaxis_title='Date',
        template='plotly_white',
        yaxis=dict(
            autorange=True,
        ),
        yaxis2=dict(
            range=[0,10000],
            domain=[0, 0.1]
        )

    )

    # 設置y軸標籤
    fig.update_yaxes(title_text='Price', secondary_y=False)
    fig.update_yaxes(title_text='Volume', secondary_y=True)

    return fig

def get_distinct_years_and_months():
    # 獲取所有不同的年份和月份
    distinct_years = collection_future_info.distinct('Year')
    distinct_months = collection_future_info.distinct('Month')
    return distinct_years, distinct_months

def get_distinct_values(collection, fields):
    """
    獲取所有不同的值
    :param collection: MongoDB 集合
    :param fields: 字段名稱或字段名稱列表
    :return: 單個字段的不同值或多個字段的不同值字典
    """
    if isinstance(fields, list):
        distinct_values = {field: collection.distinct(field) for field in fields}
        return distinct_values
    else:
        return collection.distinct(fields)

def get_commodity_id(collection, filters):
    # 根據過濾條件找到對應的 COMMODITYID
    result = collection.find_one(filters)
    return result['COMMODITYID'] if result else None

def get_available_dates( commodity_id):
    # 使用聚合管道獲取有數據的日期
    pipeline = [
        {"$match": {"COMMODITYID": commodity_id}},
        {"$project": {"date": {"$dateTrunc": {"date": "$TimeStamp", "unit": "day"}}, "COMMODITYID": 1}},
        {"$group": {"_id": {"date": "$date", "COMMODITYID": "$COMMODITYID"}}},
        {"$sort": {"_id.date": 1, "_id.COMMODITYID": 1}}
    ]
    result = collection.aggregate(pipeline)
    
    # 调试信息
    dates = [doc['_id']['date'] for doc in result]
    print(f"Available dates for commodity_id {commodity_id}: {dates}")
    
    # 确保返回的日期是 datetime.date 对象
    return [date.date() for date in dates]

# 主函数
def main():
    st.header(" Data")
    placeholder = st.empty()  # 创建一个占位符

    data_type = st.sidebar.selectbox("選擇數據種類", ["期貨", "選擇權"])

    if data_type == "期貨":
        # 獲取所有不同的年份和月份
        distinct_values = get_distinct_values(collection_option_info, ['Year', 'Month'])

        # 在側邊欄添加選擇框讓用戶選擇年份和月份
        selected_year = st.sidebar.selectbox("請選擇年份", distinct_values['Year'])
        selected_month = st.sidebar.selectbox("請選擇月份", distinct_values['Month'])

        # 根據用戶選擇的年份和月份找到對應的 COMMODITYID
        commodity_id = get_commodity_id(collection_future_info, {'Year': selected_year, 'Month': selected_month})
    
    elif data_type == "選擇權":
        distinct_values = get_distinct_values(collection_option_info, ['Year', 'Month', 'WeekMonth', 'StrikePrice', 'OptionType'])

        # 在側邊欄添加選擇框讓用戶選擇年份、月份、週月份、行使價和選擇權類型
        selected_year = st.sidebar.selectbox("請選擇年份", distinct_values['Year'])
        selected_month = st.sidebar.selectbox("請選擇月份", distinct_values['Month'])
        selected_weekmonth = st.sidebar.selectbox("請選擇週月份", distinct_values['WeekMonth'])
        selected_strike_price = st.sidebar.selectbox("請選擇行使價", distinct_values['StrikePrice'])
        selected_option_type = st.sidebar.selectbox("請選擇選擇權類型", distinct_values['OptionType'])
        commodity_id = get_commodity_id(collection_option_info, {
            'Year': selected_year,
            'Month': selected_month,
            'WeekMonth': selected_weekmonth,
            'StrikePrice': selected_strike_price,
            'OptionType': selected_option_type
        })
    # 实时刷新数据
    while commodity_id:
        st.text(f"COMMODITYID: {commodity_id}")
        # 獲取有數據的日期
        available_dates = get_available_dates(commodity_id)
        # available_dates = [date.date() for date in available_dates]

        # 添加日期選擇器讓用戶選擇資料日期
        selected_date = st.sidebar.date_input("請選擇資料日期", min_value=min(available_dates), max_value=max(available_dates), value=min(available_dates))
        start_date = datetime.combine(selected_date, datetime.min.time())
        end_date = start_date + timedelta(days=1)

        while True:
            query = {
                'COMMODITYID': commodity_id,
                'TimeStamp': {
                    '$gte': start_date,
                    '$lt': end_date
                }
            }
            
            df = get_data(query)
            df_option = organize_data(df)
            
            if not df.empty:
                # 使用佔位符顯示圖表
                placeholder.plotly_chart(draw_plot(df_option, query))
            
            time.sleep(60)  # 每60秒刷新一次

if __name__ == "__main__":
    main()