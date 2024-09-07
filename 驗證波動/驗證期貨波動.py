import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy.stats as stats

plt.rc('font', family='Heiti TC')
import os
import tempfile

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

df_all = pd.read_pickle('./2015~2024month_future_price_df.pkl')


# 計算波動
df_all['每日價格波動率'] = np.log(
    df_all['收盤價'] / df_all['開盤價']
)
df_all['小時'] = df_all['時間'].dt.hour

df_all['星期'] = df_all['時間'].dt.weekday


df = df_all[(df_all['小時'] >= 8) & (df_all['小時'] <= 13) ]




# %%
def f_test(df_0, df_10):

    # 步骤 1: 计算两个日期区间内每日波动率的方差
    variance_0 = df_0['每日價格波動率'].var(ddof=1) * 252*24*60
    variance_10 = df_10['每日價格波動率'].var(ddof=1) * 252*24*60

    # # 确保 variance_0 是较大的方差
    # if variance_0 < variance_10:
    #     variance_0, variance_10 = variance_10, variance_0
    #     df_dotm_0, df_dotm_10 = df_0, df_10

    # 步骤 2: 使用 F-test 公式来计算 F 值
    F_value = variance_0 / variance_10

    # 步骤 3: 计算自由度
    df_numerator = len(df_0) - 1
    df_denominator = len(df_10) - 1

    # 使用 scipy 的 F 分布来计算 p 值
    p_value = 1 - stats.f.cdf(F_value, df_numerator, df_denominator)

    # # 判断差异是否显著
    # variance_0 = round(variance_0, 5)
    # variance_10 = round(variance_10, 5)
    # F_value = round(F_value, 5)
    # p_value = round(p_value, 5)

    return variance_0, variance_10, F_value, p_value


def perform_f_test(
    df,
    option_type,
    price_type,
    day_1,
    day_2,
    markdown_file_path,
    images_folder_path,
):
    # 根据買權/賣權类型和價性筛选数据


    # 分成0跟10天
    df_0 = df[df['周月註記'] == '月']
    df_10 = df[df['周月註記'].isna()]
    
    # 星期一
    df_0 = df[df['星期'] == 0]
    df_2 = df[df['星期'] == 1]

    # 执行 f_test 并打印结果

    variance_0, variance_10, F_value, p_value = f_test(df_0=df_0, df_10=df)

    print(f'週選結算年化波動率: {np.sqrt(variance_0)}\n其他年化波動率: {np.sqrt(variance_10)}\nF 值: {F_value}\np 值: {p_value}')
    degreefreedom_0 = len(df_0) - 1
    degreefreedom_10 = len(df_10) - 1

    # # 柱狀圖f,p
    # plt.bar(['F 值', '-log10(p) 值'], [F_value, -np.log10(p_value)])
    # plt.title('F-test 结果')
    # plt.show()

    alpha = 0.05  # 常用的显著性水平
    if p_value < alpha:
        print('波率存在顯著差異')
    else:
        print('波率不存在顯著差異')





    # 箱線圖绘制并保存
    plt.figure(figsize=(8, 6))
    plt.boxplot(
        [df_0['每日價格波動率'], df_10['每日價格波動率']],
        labels=[f'週選結算', f'其他'],
    )
    plt.title('波動比較')
    plt.ylabel('值')
    plt.savefig('./images/期貨.png')
    plt.close()







perform_f_test(
    df_all,
    option_type,
    price_type,
    day_1=1,
    day_2=9,
    markdown_file_path=markdown_file_path,
    images_folder_path=images_directory,
)
