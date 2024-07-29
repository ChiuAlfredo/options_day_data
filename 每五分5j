import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from openpyxl.utils.cell import get_column_letter
import os

# 讀取Excel文件
df = pd.read_excel("data.xlsx")

# 假設 '時間' 列包含日期和時間，將其轉換為datetime格式
df['Datetime'] = pd.to_datetime(df['時間'], errors='coerce')

# 提取日期和時間部分
df['Date'] = df['Datetime'].dt.date
df['Time'] = df['Datetime'].dt.time

# 定義時間範圍
start_time = pd.to_datetime('09:01:00').time()
end_time = pd.to_datetime('13:29:00').time()

# 篩選時間範圍
timefiltered_df = df[(df['Time'] >= start_time) & (df['Time'] <= end_time)].copy()

# 按履約價和每5分鐘分組計算總和
grouped_df = timefiltered_df.groupby(['履約價','買賣權', pd.Grouper(key='Datetime', freq='5min')]).sum().reset_index()

# 將結果輸出到新的Excel文件
output_file = "C:/Users/ASUS/Desktop/grouped_data_by_strike_price1.xlsx"
grouped_df.to_excel(output_file, sheet_name='Grouped Data', index=False)

print(f"數據已成功輸出到 {output_file}")
