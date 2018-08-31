# coding: utf-8
import pandas as pd
from time import strftime

def read_data_from_csv(file_name):
    df = pd.read_csv('../DataSet/' + file_name + '.csv', header=0)
    return df

def cal_user_daily_load(file_name='for_clustering'):
    df = read_data_from_csv(file_name)
    df['Week_ID'] = pd.to_datetime(df['reportTime']).dt.weekday + 1
    df_user_group = df.groupby('userID')
    userID = df_user_group.groups.keys()
    users_dailyload = pd.DataFrame()
    
    for user in userID:
        df_weekday_group = df_user_group.get_group(user).groupby('Week_ID')
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
            temp_df.insert(loc=0, column='Week_ID', value=day)
            temp_df.insert(loc=0, column='User_ID', value=user)
            dailyload = dailyload.append(temp_df)

        dailyload['Reporttime'] = strftime('%Y-%m-%d %H:%M')
        users_dailyload = users_dailyload.append(dailyload)
    users_dailyload.to_csv(r'../DataSet/user_dailyload.csv', index=False)
    users_dailyload.to_csv(r'../DataSet_backup/user_dailyload_' + strftime('%Y-%m-%d_%H-%M') + '.csv', index=False)

def run(file_name='for_clustering'):
    cal_user_daily_load(file_name)