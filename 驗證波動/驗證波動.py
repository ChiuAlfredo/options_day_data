import pandas as pd

df_all = pd.read_csv('./驗證波動/txo202102-202104最高價最低價.csv')

df_all['買賣權'] = df_all['證券代碼'].apply(lambda x: 'C' if 'C' in x else ('P' if 'P' in x else '未知'))

df_all['履約價'] = df_all['證券代碼'].str[-5:].astype(int)

option_title = df_all['證券代碼'].unique().tolist()

df_all['日期'] = pd.to_datetime(df_all['年月日'], format='%Y%m%d')


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

# 应用函数来创建新列 '價性'
df_all['價性'] = df_all.apply(classify_option, axis=1)

df_all['每日價格波動率'] =( df_all['選擇權當日最高價'] - df_all['選擇權當日最低價'])/df_all['選擇權當日最低價']


max_dates = df_all.groupby('證券代碼')['日期'].max().reset_index()
max_dates.rename(columns={'日期': '最大日期'}, inplace=True)

# 将最大日期信息合并回原始 DataFrame
df_all = pd.merge(df_all, max_dates, on='證券代碼')

# 步骤 2: 计算剩余天数
df_all['剩餘天數'] = (df_all['最大日期'] - df_all['日期']).dt.days


# 篩選掉剩餘天數大於10的資料
df_all = df_all[df_all['剩餘天數'] <= 10]



#%%

# 選擇價性為深度價外的資料
df_dotm = df_all[df_all['價性'] == '深度價外 (DOTM)']

# 分成0跟10天
df_dotm_0 = df_dotm[df_dotm['剩餘天數'] == 0]
df_dotm_10 = df_dotm[(df_dotm['剩餘天數'] > 0)& (df_dotm['剩餘天數'] <= 10)]


#%%

# 選擇權價格為價平的資料
df_atm = df_all[df_all['價性'] == '價平 (ATM)']

# 分成0跟10天
df_atm_0 = df_atm[df_atm['剩餘天數'] == 0]
df_atm_10 = df_atm[(df_atm['剩餘天數'] > 0)& (df_atm['剩餘天數'] <= 1)]

f_test(df_0 = df_dotm_0, df_10 = df_dotm_10)

#%%

# 選擇權價格為價內的資料
df_itm = df_all[df_all['價性'] == '價內 (ITM)']

# 分成0跟10天
df_itm_0 = df_itm[df_itm['剩餘天數'] == 0]
df_itm_10 = df_itm[(df_itm['剩餘天數'] > 0)& (df_itm['剩餘天數'] <= 10)]

f_test(df_0 = df_itm_0, df_10 = df_itm_10)


#%%
# 選擇權價格為深度價外的資料
df_dotm = df_all[df_all['價性'] == '深度價外 (DOTM)']

# 分成0跟10天
df_dotm_0 = df_dotm[df_dotm['剩餘天數'] == 0]
df_dotm_10 = df_dotm[(df_dotm['剩餘天數'] > 0)& (df_dotm['剩餘天數'] <= 1)]

f_test(df_0 = df_dotm_0, df_10 = df_dotm_10)


#%%
import scipy.stats as stats


def f_test(df_0,df_10):

    # 步骤 1: 计算两个日期区间内每日波动率的方差
    variance_0 = df_0['每日價格波動率'].var()
    variance_10 = df_10['每日價格波動率'].var()

    # 确保 variance_0 是较大的方差
    if variance_0 < variance_10:
        variance_0, variance_10 = variance_10, variance_0
        df_dotm_0, df_dotm_10 = df_0, df_10

    # 步骤 2: 使用 F-test 公式来计算 F 值
    F_value = variance_0 / variance_10

    # 步骤 3: 计算自由度
    df_numerator = len(df_0) - 1
    df_denominator = len(df_10) - 1

    # 使用 scipy 的 F 分布来计算 p 值
    p_value = 1 - stats.f.cdf(F_value, df_numerator, df_denominator)

    print(f'F 值: {F_value}')
    print(f'p 值: {p_value}')

    # 判断差异是否显著
    alpha = 0.05  # 常用的显著性水平
    if p_value < alpha:
        print('波率存在顯著差異')
    else:
        print('波率不存在顯著差異')
