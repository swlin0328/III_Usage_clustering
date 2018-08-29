
# coding: utf-8
from sklearn.cluster import KMeans
from mysql_model import sql4cluster
import pandas as pd
from time import strftime
import os

def read_data_from_csv(file_path):
    df = pd.read_csv(file_path, header=0)
    return df

def group_user_dailyload(model_name, file_path='./result/user_dailyload.csv'):
    df = read_data_from_csv(file_path)
    df_user_group = df.groupby('User_id')
    userId = df_user_group.groups.keys()
    km = sql4cluster(model_name=model_name)
    km_model = km.load_model_from_pkl()

    user_dailyload_group = pd.DataFrame()
    for user in userId:
        dailyload2group = pd.DataFrame()
        user_group = df_user_group.get_group(user)
        input_X = user_group.iloc[:, 4:100] # recorded power consumption
        group = km_model.predict(input_X)

        dailyload2group['Week_id'] = user_group['Week_id']
        dailyload2group['Group_id'] = group
        dailyload2group['Reporttime'] = strftime('%Y-%m-%d %H:%M')
        dailyload2group.insert(loc=0, column='User_id', value=user)
        user_dailyload_group = user_dailyload_group.append(dailyload2group)

    user_dailyload_group.to_csv(r'./result/user_group_relation.csv', index=False)