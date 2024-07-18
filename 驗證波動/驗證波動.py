import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy.stats as stats

plt.rc('font', family='Heiti TC')
import os
import tempfile

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

df_all = pd.read_csv('./驗證波動/txo202101-202104.csv', encoding='utf-8')

df_all['買賣權'] = df_all['證券代碼'].apply(
    lambda x: 'C' if 'C' in x else ('P' if 'P' in x else '未知')
)

df_all['履約價'] = df_all['證券代碼'].str[-5:].astype(int)

option_title = df_all['證券代碼'].unique().tolist()

df_all['日期'] = pd.to_datetime(df_all['年月日'], format='%Y%m%d')

# 設定0為0.1
df_all.loc[df_all['選擇權開盤價'] == 0, '選擇權開盤價'] = 0.1
df_all.loc[df_all['選擇權收盤價'] == 0, '選擇權收盤價'] = 0.1


def classify_option(row):
    k_s_ratio = row['履約價'] / row['標的證券價格']
    if row['買賣權'] == 'C':  # 買權
        if 0.94 <= k_s_ratio < 0.97:
            return '深度價內 (DITM)'
        elif 0.97 <= k_s_ratio < 0.99:
            return '價內 (ITM)'
        elif 0.99 <= k_s_ratio < 1.01:
            return '價平 (ATM)'
        elif 1.01 <= k_s_ratio < 1.03:
            return '價外 (OTM)'
        elif 1.03 <= k_s_ratio < 1.06:
            return '深度價外 (DOTM)'
    elif row['買賣權'] == 'P':  # 賣權
        if 1.03 <= k_s_ratio < 1.06:
            return '深度價內 (DITM)'
        elif 1.01 <= k_s_ratio < 1.03:
            return '價內 (ITM)'
        elif 0.99 <= k_s_ratio < 1.01:
            return '價平 (ATM)'
        elif 0.97 <= k_s_ratio < 0.99:
            return '價外 (OTM)'
        elif 0.94 <= k_s_ratio < 0.97:
            return '深度價外 (DOTM)'
    return '未知'


## 價性 k/s
df_all['價性'] = df_all.apply(classify_option, axis=1)


# 为每个證券代碼计算最大日期
df_all['最大日期'] = df_all.groupby('證券代碼')['日期'].transform(max)

# 初始化剩餘天數列
df_all['剩餘有資料的天數'] = 0

# 对每个證券代碼进行迭代
for code in df_all['證券代碼'].unique():
    # 获取当前證券代碼的所有日期
    dates = df_all[df_all['證券代碼'] == code]['日期']
    max_date = df_all[df_all['證券代碼'] == code]['最大日期'].iloc[0]

    # 对每个日期进行迭代，计算到最大日期之间实际有数据的日期数量
    for index, row in df_all[df_all['證券代碼'] == code].iterrows():
        # 计算当前日期到最大日期之间的所有日期
        all_dates = pd.date_range(start=row['日期'], end=max_date)

        # 计算实际有数据的日期数量
        actual_dates_count = dates.isin(all_dates).sum()

        # 更新剩餘有資料的天數
        df_all.at[index, '剩餘天數'] = actual_dates_count 


# 計算波動
df_all['每日價格波動率'] = np.log(
    df_all['選擇權收盤價'] / df_all['選擇權開盤價']
)

df_all.to_csv(
    './驗證波動/txo202101-202104_test.csv', encoding='utf-8-sig', index=False
)


# %%
def f_test(df_0, df_10):

    # 步骤 1: 计算两个日期区间内每日波动率的方差
    std_0 = df_0['每日價格波動率'].std(ddof=1) * np.sqrt(252)
    std_10 = df_10['每日價格波動率'].std(ddof=1) * np.sqrt(252)

    variance_0 = std_0**2
    variance_10 = std_10**2

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

    # 判断差异是否显著
    variance_0 = round(variance_0, 5)
    variance_10 = round(variance_10, 5)
    F_value = round(F_value, 5)
    p_value = round(p_value, 5)

    return std_0, std_10, F_value, p_value


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
    df_filtered = df[
        (df['買賣權'] == option_type) & (df['價性'] == price_type)
    ]

    # 分成0跟10天
    df_0 = df_filtered[df_filtered['剩餘天數'] <= day_1]
    df_10 = df_filtered[
        (df_filtered['剩餘天數'] > day_1) & (df_filtered['剩餘天數'] <= day_2)
    ]

    # 执行 f_test 并打印结果

    std_0, std_10, F_value, p_value = f_test(df_0=df_0, df_10=df_10)

    degreefreedom_0 = len(df_0) - 1
    degreefreedom_10 = len(df_10) - 1

    # # 柱狀圖f,p
    # plt.bar(['F 值', '-log10(p) 值'], [F_value, -np.log10(p_value)])
    # plt.title('F-test 结果')
    # plt.show()
    print(f'{option_type} {price_type}\n')
    print(
        f'結算{day_1}內年化波動: {std_0}\n結算{day_2}內波動年化波動: {std_10}\n f值: {F_value}\n p值: {p_value}'
    )
    alpha = 0.05  # 常用的显著性水平
    if p_value < alpha:
        print('年化波動存在顯著差異')
    else:
        print('年化波動不存在顯著差異')

    # 确保图表的存储文件夹存在
    if not os.path.exists(images_folder_path):
        os.makedirs(images_folder_path)

    # 图表文件路径
    temp_image_path = os.path.join(
        images_folder_path,
        f"{option_type}_{price_type}_{day_1}_{day_2}.png".replace(" ", "_")
        .replace("(", "")
        .replace(")", ""),
    )

    # 箱線圖绘制并保存
    plt.figure(figsize=(8, 6))
    plt.boxplot(
        [df_0['每日價格波動率'], df_10['每日價格波動率']],
        labels=[f'結算{0}-{day_1}日內', f'結算{day_2+1}-{day_2}日內'],
    )
    plt.title(f'{option_type} {price_type}日報酬箱線圖')
    plt.ylabel('日報酬')
    plt.savefig(temp_image_path)
    plt.close()

    # 图表文件相对路径，相对于Markdown文件的位置
    relative_image_path = os.path.join(
        "images",
        f"{option_type}_{price_type}_{day_1}_{day_2}.png".replace(" ", "_")
        .replace("(", "")
        .replace(")", ""),
    )

    # 将结果写入Markdown文件
    with open(markdown_file_path, 'a') as md_file:
        md_file.write(f"## {option_type} {price_type}\n\n")
        md_file.write(
            f'''
            - 結算{0}-{day_1}日內年化波動: {std_0}\n
            - 結算{day_1+1}-{day_2}日內年化波動: {std_10}\n
            - 結算{0}-{day_1}日內自由度: {degreefreedom_0}\n
            - 結算{day_1+1}-{day_2}日內自由度: {degreefreedom_10}\n
            - f值: {F_value}\n
            - p值: {p_value}\n
            - alpha值: {alpha}\n\n'''
        )
        md_file.write(f"""- **結論**: {(
            '年化波動存在顯著差異' if p_value < alpha else '年化波動不存在顯著差異'
        )}\n\n""")
        md_file.write(
            f"![{option_type} {price_type} 每日報酬率比較]({relative_image_path})\n\n"
        )


# 假设 df_all 是包含所有数据的 DataFrame
# 定义不同的買權/賣權类型和價性类型
option_types = ['C', 'P']
price_types = [
    '深度價內 (DITM)',
    '價內 (ITM)',
    '價平 (ATM)',
    '價外 (OTM)',
    '深度價外 (DOTM)',
]

# 主目录和图表文件夹路径
main_directory = "./驗證波動"
images_directory = os.path.join(main_directory, "images")

# Markdown文件路径
markdown_file_path = os.path.join(main_directory, "report.md")

# 初始化Markdown文件
if os.path.exists(markdown_file_path):
    os.remove(markdown_file_path)  # 如果文件已存在，先删除

# 循环遍历不同的買權/賣權类型和價性类型，执行 f_test
for option_type in option_types:
    # option_type = option_types[0]
    for price_type in price_types:
        # price_type = price_types[1]
        perform_f_test(
            df_all,
            option_type,
            price_type,
            day_1=1,
            day_2=9,
            markdown_file_path=markdown_file_path,
            images_folder_path=images_directory,
        )
