#coding=utf-8 
#!/usr/bin/env python
import pymssql, cPickle
from time import strftime
import types
import tempfile
from ast import literal_eval as make_tuple


class cluster4sql():
    """
    Class for uploading preprocessed result to MSSQL.

    Member function
    ----------
    str2tuple : convert the app_loc stored on MSSQL to tuple format.
    
    """
        
    def __init__(self, user="III_Cluster", password="III_clustering", database="usage_db", host_address='140.92.174.21\SQLEXPRESS01', port='1433', meter_name=None):
        self.user = user
        self.password = password
        self.database = database
        self.host_address = host_address
        self.created_time = strftime('%Y-%m-%d_%H_%M')
        self.port = port
        self.meter_name = meter_name
        if meter_name is None:
            self.meter_name = {"main":[0], "others":[1], "television":[2], "fridge":[3, 1002], "air conditioner":[4, 1004], "bottle warmer":[5], "washing machine":[6]}

        self.msSQL_connect()
        self.chk_environment()

    def msSQL_connect(self):
        print('=====================================================')
        print('======== Connect to the remote msSQL server ========')
        print('=====================================================')
        print('Time : {}\n'.format(strftime('%Y-%m-%d_%H_%M')))
        self.db = pymssql.connect(server=self.host_address, port=self.port, user=self.user, password=self.password, database=self.database, charset="utf8")
        self.cursor = self.db.cursor()

    def disconnect(self):
        self.db.close()
        print('=====================================================')
        print('============ Close the remote connection ============')
        print('=====================================================')

    def chk_environment(self):
        if self.db is None or self.cursor is None:
            print('False to connect')

    def str2tuple(self, target_str):
        target_tuple = make_tuple(target_str)
        return target_tuple

    def create_table(self, sql4create_table):
        """
        Parameters
        ----------
        sql4create_table : MSSQL command to create table on existing database.

        """
        self.cursor.execute(sql4create_table)
        self.db.commit()

    def drop_table(self, sql4drop_table):
        """
        Parameters
        ----------
        sql4drop_table : MSSQL command to drop table on existing database.

        """     
        self.cursor.execute(sql4drop_table)
        self.db.commit()

    def init_tables(self):
        cmd4building  = """CREATE TABLE building_table
                            (building int NOT NULL,
                            PRIMARY KEY (building));"""
        
        cmd4usage     = """CREATE TABLE usage_table
                            (report_date datetime NOT NULL,
                            building int NOT NULL,
                            appliance_code int NOT NULL,
                            turn_on_time datetime NOT NULL,
                            turn_off_time datetime NULL,
                            interior_location nvarchar(20) NOT NULL,
                            FOREIGN KEY (building) REFERENCES building_table(building),
                            PRIMARY KEY (report_date, building, appliance_code, turn_on_time, interior_location));"""
        
        cmd4represent = """CREATE TABLE representation_blob
                            (record_date datetime NOT NULL,
                            building int NOT NULL, 
                            FOREIGN KEY (building) REFERENCES building_table(building),
                            PRIMARY KEY (record_date, building),
                            sequence_blob varchar(max) not null); """
        
        sql_tables = [cmd4building, cmd4usage, cmd4represent]
        
        for table in sql_tables:
            self.create_table(table)
        

    def search_data(self, sql4search_data=None, building=None, report_date=None):
        """
        Parameters
        ----------
        sql4search_data : MSSQL command to search data on table, and then printing the data.

        Default command is to search the representation blob for specific building and date.
        
        """     
        if sql4search_data is None:
            sql4search_data = "SELECT * FROM representation_blob WHERE building = %s and report_date = %s"
            self.cursor.execute(sql4search_data, (building, report_date, ))
        else:
            self.cursor.execute(sql4search_data)
            
        row = self.cursor.fetchone()
        result = []
        while row:
            result.append(row)
            print("Get row... ", row)
            row = self.cursor.fetchone()

        return result

    def insert_data(self, sql4insert_data=None, building=None, data=None):
        """
        Parameters
        ----------
        sql4insert_data : MSSQL command to search data on table.

        Default command is to insert building_id to building_table, which is the primary key on the main table.
        
        """       
        if sql4insert_data is None:
            sql4insert_data = "INSERT INTO building_table VALUES (%d)"
            self.cursor.execute(sql4insert_data, (building,))
        else:
            self.cursor.execute(sql4insert_data, data)
        self.db.commit()

    def delete_data(self, sql4delete_data=None, building=None, report_date=None):
        """
        Parameters
        ----------
        sql4delete_data : MSSQL command to delete data on table.

        Default command is to delete all the data of target building on the current tables.
        
        """
        if sql4delete_data is None:
            sql_table = ['representation_blob', 'usage_table', 'building_table']
            for table in sql_table:
                sql4delete_data = "DELETE FROM " + table + " WHERE building = " + str(building)
                self.cursor.execute(sql4delete_data)
        else:
            self.cursor.executemany(sql4delete_data)
        
        self.db.commit()

    def insert_representation_blob(self, usage_representation, building_id):
        """
        Parameters
        ----------
        usage_representation : pd.Series, which is the preprocess result for usage representation.
        
        """
        datetime_index = usage_representation.index[0].strftime("%Y/%m/%d")
        representation_blob = cPickle.dumps(usage_representation)
        add_blob = "INSERT INTO Representation_Blob (record_date, building, sequence_blob) VALUES (%s, %d, %s)"
        self.cursor.execute(add_blob, (datetime_index, building_id, representation_blob,))
                
        self.db.commit()

    def delete_blob(self):
        sql4delete_blob = "DELETE FROM representation_blob WHERE building = %s AND report_date = %s"
        self.cursor.executemany(sql4delete_blob, (building, report_date))
        self.db.commit()

    def search_representation_blob(self, building_id, start_date, end_date):
        sql_cmd = "SELECT sequence_blob FROM representation_blob WHERE building = %d and record_date between %s and %s"
        
        self.cursor.execute(sql_cmd, (building_id, start_date, end_date))
        row = self.cursor.fetchone()
        representation_blobs = []
        
        while row:
            representation_blobs.append(row)
            row = self.cursor.fetchone()

        return representation_blobs

    def load_representation(self, building_id, start_date, end_date='2030/12/31'):
        """
        Parameters
        ----------
        representation : pd.Series, which is loaded from the binary stream on MSSQL.
        
        """
        representation_blobs = self.search_representation_blob(building_id, start_date, end_date)
        representation = []
        
        for blob in representation_blobs:
            seq = cPickle.loads(str(blob[0]))
            representation.append(seq)
            
        return representation

    def result2db(self, usage_representation, app_loc):
        """
        Parameters
        ----------
        app_loc : tuple, which record the location of the target appliance with the form of (x, y, z)
        is_target_appliance : bool
        
        """
        
        sql_buildings = self.search_data("SELECT * FROM building_table")
        sql_buildings = [data[0] for data in sql_buildings]
                                         
        for building_id in usage_representation.keys():
            if sql_buildings is None or building_id not in sql_buildings:
                self.insert_data(building=building_id)

            building_data = usage_representation[building_id]

            year_data = building_data.groupby(building_data.index.year)
            year_index = year_data.groups.keys()

            for year_idx in year_index:
                target_year = year_data.get_group(year_idx)
                month_data = target_year.groupby(building_data.index.month)
                month_index = month_data.groups.keys()

                for month_idx in month_index:
                    target_month = month_data.get_group(month_idx)
                    day_data = target_month.groupby(building_data.index.day)
                    day_index = day_data.groups.keys()
                
                    for day_idx in day_index:
                        sql_cmd = ""
                        day_seq = day_data.get_group(day_idx)
                        self.save_seq2db(day_seq, building_id, app_loc)
                               
        self.db.commit()

    def save_seq2db(self, day_seq, building_id, app_loc):
        report_date = day_seq.index[0].strftime("%Y/%m/%d")

        if len(self.search_data("SELECT * FROM representation_blob WHERE record_date = " + report_date + " and building =" + str(building_id))) < 1:
            return
        
        for time_idx, seq in day_seq.iteritems():
            for app in seq:                  
                appliance_code = abs(app)
                            
                if app > 0:
                    turn_on_time = time_idx.strftime("%Y/%m/%d %H:%M")
#                   interior_location = app_loc[building_id][appliance]
                    interior_location = (1, 1, appliance_code)
                    sql_cmd = "INSERT INTO usage_table VALUES (%s, %d, %d, %s, %s, %s)"
                    applance_data = (report_date, building_id, appliance_code, turn_on_time, "00:00", str(interior_location),)
                    self.insert_data(sql_cmd, data=applance_data)
                else:                 
                    turn_off_time = time_idx.strftime("%Y/%m/%d %H:%M")
                    sql_cmd = "UPDATE usage_table SET turn_off_time = %s WHERE report_date = %s and appliance_code = %d and building = %d"
                    self.cursor.execute(sql_cmd, (turn_off_time, report_date, appliance_code, building_id))
                
        self.insert_representation_blob(day_seq, building_id)
