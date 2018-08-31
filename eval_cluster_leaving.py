import os
from os.path import join
import numpy as np
import pandas as pd
from time import strftime
PATH = join(os.path.expanduser('~') , 'Desktop', 'Clustering_analysis')

def run():
    df = pd.read_csv(join(PATH, 'DataSet', 'cluster_with_label.csv'))
    load_range = np.load(join(PATH, 'DataSet', 'eval_cluster_loadrange.npy')).item()
    eval_cluster_leaving(df, load_range)

def eval_cluster_leaving(df, load_range, cluster=5):
    """
        input : df('cluster_with_label.csv'), load_range('eval_cluster_loadrange.npy')
        return : for example: ./DataSet/eval_cluster_leaving.csv
    """
    labels = df['Group_ID']
    df = df.drop(['uuid', 'Reporttime', 'Group_ID', 'userId'], axis=1)
    result = pd.DataFrame({'Group_ID':[], 'c_cost':[], 'c_tload':[], 'c_pload':[], 'dist_cost':[], 'dist_tload':[], 
                            'dist_pload':[], 'Reporttime':[]})   
    for center in range(cluster):
        center_sum = load_range['center_'+str(center)]['sum']
        center_max = load_range['center_'+str(center)]['max']
        temp_sum = abs(df[labels==center]['wSum']-center_sum)
        temp_sum = pd.DataFrame(temp_sum)
        temp_max = abs(df[labels==center]['wMax']-center_max)
        temp_min = abs(df[labels==center]['wMin']-center_max)
        temp_max_value = max(temp_max.max(), temp_min.max()) # the max dist between obs and center within the same group happens at min or max
        temp = pd.DataFrame({'Group_ID':[center], 'c_cost':[center_sum], 'c_tload':[center_sum], 'c_pload':[center_max], 
                            'dist_cost':[temp_sum.max()[0]], 'dist_tload':[temp_sum.max()[0]*1.5], 'dist_pload':[temp_max_value], 
                            'Reporttime':[strftime('%Y-%m-%d %H:%M')]})
        result = result.append(temp)
    print '==== save eval_cluster_loadrange in ./DataSet/leaving_group.csv ===='
    result.to_csv(join(PATH, 'DataSet', 'leaving_group.csv'), index=False,
                    columns=['Group_ID', 'c_cost', 'c_tload', 'c_pload', 'dist_cost', 'dist_tload', 'dist_pload', 'Reporttime'])  
    return result

if __name__ == '__main__':
    run()