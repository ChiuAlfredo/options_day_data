import pickle
import pandas as pd
import numpy as np
from datetime import timedelta
from scipy.stats import ttest_ind
import matplotlib.pyplot as plt
import scipy.stats as stats
import os
import tempfile
import matplotlib.pyplot as plt
from matplotlib import rcParams

# Load the data from the pickle file
with open(r"C:\Users\ASUS\Desktop\data_endln.pkl", "rb") as f:
    df_all = pickle.load(f)

# 設定0為0.1
df_all.loc[df_all['選擇權_開盤價'] == 0, '選擇權_開盤價'] = 0.1
df_all.loc[df_all['選擇權_收盤價'] == 0, '選擇權_收盤價'] = 0.1

# 初始化剩餘天數列
df_all['剩餘有資料的天數'] = 0

# 計算波動
df_all['每日價格波動率'] = np.log(df_all['選擇權_收盤價'] / df_all['選擇權_開盤價'])

# %%
def f_test(df_0, df_10):

    # 步骤 1: 计算两个日期区间内每日波动率的方差
    variance_0 = df_0['每日價格波動率'].var(ddof=1) / np.sqrt(252)
    variance_10 = df_10['每日價格波動率'].var(ddof=1) / np.sqrt(252)

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
    df_filtered = df[
        (df['買賣權別'] == option_type) & (df['價平檔位'] == price_type)
    ]

    # 分成0跟10天
    df_0 = df_filtered[df_filtered['整天數'] <= day_1]
    df_10 = df_filtered[
        (df_filtered['整天數'] > day_1) & (df_filtered['整天數'] <= day_2)
    ]

    # 执行 f_test 并打印结果

    variance_0, variance_10, F_value, p_value = f_test(df_0=df_0, df_10=df_10)

    degreefreedom_0 = len(df_0) - 1
    degreefreedom_10 = len(df_10) - 1

    # # 柱狀圖f,p
    # plt.bar(['F 值', '-log10(p) 值'], [F_value, -np.log10(p_value)])
    # plt.title('F-test 结果')
    # plt.show()
    print(f'{option_type} {price_type}\n')
    print(
        f'結算{day_1}內波動率: {variance_0}\n結算{day_2}內波動率: {variance_10}\n f值: {F_value}\n p值: {p_value}'
    )
    alpha = 0.05  # 常用的显著性水平
    if p_value < alpha:
        print('波率存在顯著差異')
    else:
        print('波率不存在顯著差異')

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

        # 設定支持中文的字體
    rcParams['font.sans-serif'] = ['Microsoft JhengHei']
    rcParams['axes.unicode_minus'] = False

    # 箱線圖绘制并保存
    plt.figure(figsize=(8, 6))
    plt.boxplot(
        [df_0['每日價格波動率'], df_10['每日價格波動率']],
        labels=[f'結算{day_1}日內', f'結算{day_2}日內'],
    )
    plt.title('波動比較')
    plt.ylabel('值')
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
            - 結算{day_1+1}日內波動率: {variance_0}\n
            - 結算{day_2+1}日內波動率(不含{day_1+1}日內): {variance_10}\n
            - 結算{day_1}日內自由度: {degreefreedom_0}\n
            - 結算{day_2}日內自由度(不含{day_1+1}日內): {degreefreedom_10}\n
            - f值: {F_value}\n
            - p值: {p_value}\n
            - alpha值: {alpha}\n\n'''
        )
        md_file.write(f"""- **結論**: {(
            '波率存在顯著差異' if p_value < alpha else '波率不存在顯著差異'
        )}\n\n""")
        md_file.write(
            f"![{option_type} {price_type} 波動比較]({relative_image_path})\n\n"
        )


# 假设 df_all 是包含所有数据的 DataFrame
# 定义不同的買權/賣權类型和價性类型
option_types = ['C', 'P']
price_types = [50,-50,100,-100,150,-150]

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
            day_2=6,
            markdown_file_path=markdown_file_path,
            images_folder_path=images_directory,
        )
