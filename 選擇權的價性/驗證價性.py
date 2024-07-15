import datetime

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy import stats
from statsmodels.formula.api import ols

plt.rc('font', family='Heiti TC')
# 載入資料
data_df = pd.read_csv('./每一分鐘資料_整理.csv', encoding='utf-8')

data_df.replace([np.inf, -np.inf], np.nan, inplace=True)
data_df.dropna(inplace=True, subset=['real_gamma'], axis=0)


data_df['時間'] = pd.to_datetime(data_df['時間'])

data_df = data_df[data_df['時間'].dt.time > datetime.time(13, 20)]


# Step 1: Group the data
groups = data_df.groupby(['價性','買賣權'])['選擇權價格']


# Iterate over each group
for name, group in groups:
    print(f"Group Name: {name}")
    print(group.head())  # Print the first few rows of each group
    print("\n") 



# Prepare lists for ANOVA
anova_data = [group for name, group in groups]

plt.figure(figsize=(8, 6))
plt.boxplot(
    anova_data
)
# plt.ylim(-1, 1)
plt.title('delta')
plt.ylabel('值')
# plt.savefig(temp_image_path)
plt.show()
# plt.close()

statistic, p_value = stats.levene(*anova_data)
print(f'statistic:{statistic}')
print(f'p_value:{p_value}')

alpha = 0.05
if p_value <= alpha:
    print("Reject the null hypothesis of equal variances.")
else:
    print("Fail to reject the null hypothesis of equal variances.")

# Step 2: Calculate the ANOVA
f_value, p_value = stats.f_oneway(*anova_data)

print(f"F-Value: {f_value}, P-Value: {p_value}")
alpha = 0.05
if p_value <= alpha:
    print("Reject the null hypothesis - there is a significant difference between the groups.")
else:
    print("Fail to reject the null hypothesis - there is no significant difference between the groups.")



# For detailed ANOVA table using statsmodels
model = ols('value_column ~ C(group_column)', data=data_df).fit()
anova_table = sm.stats.anova_lm(model, typ=2)

print(anova_table)