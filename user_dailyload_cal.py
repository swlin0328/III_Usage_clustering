# coding: utf-8
import pandas as pd
from time import strftime

def read_data_from_csv(file_name):
    df = pd.read_csv('./data/' + file_name + '.csv', header=0)
    return df

def cal_user_daily_load(file_name='max_min_sum_w_dataSet'):
    df = read_data_from_csv(file_name)
    df['Week_id'] = pd.to_datetime(df['reportTime']).dt.weekday + 1
    df_user_group = df.groupby('userId')
    userId = df_user_group.groups.keys()
    users_dailyload = pd.DataFrame()
    
    for user in userId:
        df_weekday_group = df_user_group.get_group(user).groupby('Week_id')
        weekday = df_weekday_group.groups.keys()
        dailyload = pd.DataFrame()
        for idx, day in enumerate(weekday):
            target_group_mean = df_weekday_group.get_group(day).iloc[:, 3:99].mean() # recorded power consumption
            target_group_max = df_weekday_group.get_group(day).loc[:, 'wMax'].mean()
            target_group_min = df_weekday_group.get_group(day).loc[:, 'wMin'].mean()
            target_group_sum = df_weekday_group.get_group(day).loc[:, 'wSum'].mean()
    
            col_name = target_group_mean.index
            temp_df = pd.DataFrame(data=target_group_mean.values.reshape(1, col_name.shape[0]), columns=col_name)
            temp_df.insert(loc=0, column='avg_wMin', value=target_group_min)
            temp_df.insert(loc=0, column='avg_wMax', value=target_group_max)
            temp_df.insert(loc=0, column='avg_wSum', value=target_group_sum)
            temp_df.insert(loc=0, column='Week_id', value=day)
            temp_df.insert(loc=0, column='User_id', value=user)
            dailyload = dailyload.append(temp_df)

        dailyload['Reporttime'] = strftime('%Y-%m-%d %H:%M')
        users_dailyload = users_dailyload.append(dailyload)
    users_dailyload.to_csv(r'./result/user_dailyload.csv', index=False)