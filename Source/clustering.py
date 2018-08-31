from sklearn.cluster import KMeans
from model_storage import storage4cluster
import pandas as pd
from time import strftime

def read_data_from_csv(file_name):
    df = pd.read_csv('../DataSet/' + file_name + '.csv', header=0)
    return df

def km_clustering(num_cluster=5, file_name='for_clustering', n_init=25, max_iter=1000, model_name='Kmeans', to_pkl=True, to_sql=False):
    km = KMeans(n_clusters=num_cluster, random_state=0, n_init=n_init, max_iter=max_iter)
    df = read_data_from_csv(file_name)
    input_X = df.iloc[:, 3:99] # The recorded power consumption
    
    y_km = km.fit_predict(input_X)
    center = km.cluster_centers_
    model_name = model_name
    
    save_model(km, model_name, to_pkl, to_sql)
    save_center2csv(center)
    save_cluster2csv_with_label(df, y_km)
    return km

def save_model(model, model_name='KMeans', to_pkl=True, to_sql=False):
    model_storage = storage4cluster(model_name)
    if to_sql:
        model_storage.save2sql(model)     
    if to_pkl:
        model_storage.save_model_to_pkl(model)

def save_center2csv(center):
    group_center = pd.DataFrame(center)
    group_center = group_center.rename(index=str, columns= dict((idx, 'period_' + str(idx+1)) for idx in range(center.shape[1])))
    group_center['Reporttime'] = strftime('%Y-%m-%d %H:%M')
    group_center.to_csv(r'../DataSet/group_center.csv', index=True, index_label = 'Group_ID')
    group_center.to_csv(r'../DataSet_backup/group_center_' + strftime('%Y-%m-%d_%H-%M') + '.csv', index=True, index_label='Group_ID')

def save_cluster2csv_with_label(df, label):
    df.insert(loc=3, column='Group_ID', value=label)
    df['Reporttime'] = strftime('%Y-%m-%d %H:%M')
    df = df.drop(['reportTime'], axis=1)
    df.to_csv(r'../DataSet/for_cluster_with_label.csv', index=False)
    df.to_csv(r'../DataSet_backup/for_cluster_with_label_' + strftime('%Y-%m-%d_%H-%M') + '.csv', index=False)

def run(num_cluster=5, file_name='for_clustering', n_init=25, max_iter=1000, model_name='Kmeans', to_pkl=True, to_sql=False):
    km = km_clustering(num_cluster, file_name, n_init, max_iter, model_name, to_pkl, to_sql)
    return km