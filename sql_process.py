#!/usr/bin/python
# -*- coding: utf-8 -*-
import pymssql, cPickle
from time import strftime
import types
import tempfile

class cluster4sql():
    def __init__(self, user="", password="", database="", host_address=""):
        self.user = user
        self.password = password
        self.database = database
        self.host_address = host_address
        self.created_time = strftime('%Y-%m-%d_%H_%M')

        self.msSQL_connect()
        self.chk_environment()

    def msSQL_connect(self):
        print('=====================================================')
        print('======== Connect to the remote msSQL server ========')
        print('=====================================================')
        print('Time : {}\n'.format(strftime('%Y-%m-%d_%H_%M')))
        self.db = pymssql.connect(host=self.host_address, user=self.user, password=self.password, database=self.database, as_dict=True, charset="utf8")
        self.db.ping(True)
        self.cursor = self.db.cursor()

    def disconnect(self):
        self.db.close()
        print('=====================================================')
        print('============ Close the remote connection ============')
        print('=====================================================')
