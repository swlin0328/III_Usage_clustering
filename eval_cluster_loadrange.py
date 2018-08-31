import os 
from os.path import join
import numpy as np
import pandas as pd
PATH = join(os.path.expanduser('~') , 'Desktop', 'Clustering_analysis')

def eval_cluster_loadrange(center):
    """
        input : group_center.csv
        return : for example: {'center_1': {'max':50, 'sum': 800}}
    """
    eval_cluster_loadrange = {}
    center = center.drop(['Reporttime'], axis=1) # drop time
    center = center.T # transport 
    for c in center.columns:
        eval_cluster_loadrange.setdefault('center_'+str(c), {'max': center[c].max(), 'sum': center[c].sum()})
        #                            #'fee': group_center[center].sum()*1.25})
    print '==== save eval_cluster_loadrange in ./data/eval_cluster_loadrange.npy ===='
    np.save(join(PATH, 'DataSet', 'eval_cluster_loadrange.npy'), eval_cluster_loadrange)
    return eval_cluster_loadrange

if __name__ == '__main__':
    df = pd.read_csv(join(PATH, 'DataSet', 'group_center.csv'), index_col=0)
    print(eval_cluster_loadrange(df))


