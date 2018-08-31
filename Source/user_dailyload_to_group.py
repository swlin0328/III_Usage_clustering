
# coding: utf-8
from sklearn.cluster import KMeans
from model_storage import storage4cluster
import pandas as pd
from time import strftime
import os

def read_data_from_csv(file_path):
    df = pd.read_csv(file_path, header=0)
    return df

def group_user_dailyload(model_name, file_path='../DataSet/user_dailyload.csv'):
    df = read_data_from_csv(file_path)
    df_user_group = df.groupby('User_ID')
    userID = df_user_group.groups.keys()
    km = storage4cluster(model_name=model_name)
    km_model = km.load_model_from_pkl()

    user_dailyload_group = pd.DataFrame()
    for user in userID:
        dailyload2group = pd.DataFrame()
        user_group = df_user_group.get_group(user)
        input_X = user_group.iloc[:, 5:101] # recorded power consumption
        group = km_model.predict(input_X)

        dailyload2group['Week_ID'] = user_group['Week_ID']
        dailyload2group['Group_ID'] = group
        dailyload2group['Reporttime'] = strftime('%Y-%m-%d %H:%M')
        dailyload2group.insert(loc=0, column='User_ID', value=user)
        user_dailyload_group = user_dailyload_group.append(dailyload2group)

    user_dailyload_group.to_csv(r'../DataSet/user_group_relation.csv', index=False)
    user_dailyload_group.to_csv(r'../DataSet_backup/user_group_relation_' + strftime('%Y-%m-%d_%H-%M') + '.csv', index=False)

def run(model_name='Kmeans', file_path='../DataSet/user_dailyload.csv'):
    group_user_dailyload(model_name, file_path)