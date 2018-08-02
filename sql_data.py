#!/usr/bin/python
# -*- coding: utf-8 -*-
import MySQLdb
from time import strftime
import types
import tempfile
import pandas as pd
import os

class sql4data():
    def __init__(self, user = None, password = None, database = None, host_address = None, port = 3306):
        self.user = user
        self.password = password
        self.database = database
        self.host_address = host_address
        self.created_time = strftime('%Y-%m-%d_%H_%M')
        self.port = port
        self.mySQL_connect()
#        self.chk_environment()

    def mySQL_connect(self):
        print('=====================================================')
        print('======== Connect to the remote mySQL server ========')
        print('=====================================================')
        print('Time : {}\n'.format(strftime('%Y-%m-%d_%H_%M')))

        self.db = MySQLdb.connect(  host=self.host_address, 
                                    port=self.port, 
                                    user=self.user, 
                                    passwd=self.password,
                                    db=self.database,
                                    charset="utf8")
        self.db.ping(True)
        self.cursor = self.db.cursor()

    def disconnect(self):
        self.db.close()
        print('=====================================================')
        print('============ Close the remote connection ============')
        print('=====================================================')

    def read_data(self, start_date = '2017-08-01', end_date = '2018-07-31', file_name = 'raw_data'):
        sql_query = ("SELECT * FROM raw_training_data WHERE reporttime BETWEEN '" + start_date + "' AND '" + end_date + "'" )
        print ('Start to query from the raw_data...')
        df = pd.read_sql(sql_query, self.db)        
        if not os.path.isdir('data'):
            os.makedirs('data')
            print('The data folder was created')
        df.to_csv(os.path.join('data', file_name + '.csv'), index=False)
        print ('The raw_data.csv was saved')
        return df

"""    def chk_environment(self):
        return True

"""
