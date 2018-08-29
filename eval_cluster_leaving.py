from os.path import join
import numpy as np
import pandas as pd
from time import strftime

def eval_cluster_leaving(df, load_range, cluster=5):
    """
        input : df('cluster_with_label.csv'), load_range('eval_cluster_loadrange.npy')
        return : for example: ./data/eval_cluster_leaving.csv
    """
    labels = df['Group_ID']
    df = df.drop(['uuid', 'Reporttime', 'wMax', 'wMin', 'wSum', 'Group_ID', 'userId'], axis=1)
    df['sum']=df.sum(axis=1)
    df['max']=df.max(axis=1)
    result = pd.DataFrame({'Group_ID':[], 'c_cost':[], 'c_tload':[], 'c_pload':[], 'dist_cost':[],
                            'dist_pload':[], 'Updatetime':[]})
    # sum 
    for center in range(cluster):
        center_sum = load_range['center_'+str(center)]['sum']
        center_max = load_range['center_'+str(center)]['max']
        temp_sum = abs(df[labels==center]['sum']-center_sum)
        temp_sum = pd.DataFrame(temp_sum)
        temp_max = abs(df[labels==center]['max']-center_max)
        temp_max = pd.DataFrame(temp_max)
        temp = pd.DataFrame({'Group_ID':[center], 'c_cost':[center_sum], 'c_tload':[center_sum], 'c_pload':[center_max], 
                            'dist_cost':[temp_sum.max()[0]], 'dist_pload':[temp_max.max()[0]], 'Updatetime':[strftime('%Y-%m-%d %H:%M')]})
        result = result.append(temp)

    print '==== save eval_cluster_loadrange in ./data/eval_cluster_leaving.csv ===='
    result.to_csv(join('data', 'leaving_group.csv'), index=False)  
    return result

if __name__ == '__main__':
    df = pd.read_csv(join('data', 'cluster_with_label.csv'))
    load_range = np.load(join('data', 'eval_cluster_loadrange.npy')).item()
    eval_cluster_leaving(df, load_range)
