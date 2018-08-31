import numpy as np
import pandas as pd

def run():
    df = pd.read_csv(r'../DataSet/group_center.csv', index_col=0)
    eval_cluster_loadrange(df)

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
    print '==== save eval_cluster_loadrange in ./DataSet/eval_cluster_loadrange.npy ===='
    np.save(r'../DataSet/eval_cluster_loadrange.npy', eval_cluster_loadrange)
    return eval_cluster_loadrange

if __name__ == '__main__':
    run()

