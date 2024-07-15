import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from openpyxl.utils.cell import get_column_letter
import os

# 讀取Excel文件
df = pd.read_excel("data.xlsx")

# 將 '時間' 列轉換為datetime格式，只提取時間部分
df['Time'] = pd.to_datetime(df['時間'], errors='coerce').dt.time

# 定義時間範圍
start_time = pd.to_datetime('13:01:00').time()
end_time = pd.to_datetime('13:30:00').time()

# 篩選時間範圍內的數據
timefiltered_df = df[(df['Time'] >= start_time) & (df['Time'] <= end_time)]

# 篩選 '價性' 為 "價內" 的數據
ksdf = timefiltered_df[timefiltered_df['價性'] == "價內 (ITM)"]

# 篩選 '型態' 為 "買權" 的數據
optiondf = ksdf[ksdf['買賣權'] == "P"].copy()

# 計算 '選擇權成交價變化率' 和 '期貨價格變化率' 的相關係數並四捨五入到小數第四位
correlationTXF = optiondf['選擇權成交價變化率'].corr(optiondf['期貨價格變化率'])
roundedTXF = round(correlationTXF, 4)

# 計算 '選擇權成交價變化率' 和 '指數價格變化率' 的相關係數並四捨五入到小數第四位
correlationTW = optiondf['選擇權成交價變化率'].corr(optiondf['指數價格變化率'])
roundedTW = round(correlationTW, 4)

# 計算 '期貨價格變化率' 和 '指數價格變化率' 的相關係數並四捨五入到小數第四位
correlationTXFTW = timefiltered_df['期貨價格變化率'].corr(timefiltered_df['指數價格變化率'])
roundedTXFTW = round(correlationTXFTW, 4)

#計算資料筆數
column_TX = '選擇權成交價變化率'
TX_rows = optiondf[column_TX].shape[0]-1
column_TXFTW = '指數價格變化率'
TXFTW_rows = timefiltered_df[column_TXFTW].shape[0]-1

# 顯示相關係數 & 資料數
print(f"{roundedTXF}")
print(f"{roundedTW}")
print(f"各{TX_rows}")
print(f"{roundedTXFTW}")
print(f"各{TXFTW_rows}")
