#!/usr/bin/python
# -*- coding: utf-8 -*-
import cPickle
from time import strftime
import pandas as pd
import numpy as np
from sql_data import sql4data
import os
import math

class data4cluster():
    def __init__(self):
        self.buliding_df = {}
        self.building_switch = {}
        self.building_representation = {}
        
    def read_data_from_sql(self, start_date = '2017-08-01', end_date = '2018-07-31', file_name = 'raw_data', cols = None):
        sql_db = sql4data()
        self.df = sql_db.read_data( start_date, end_date, file_name )
        if cols is not None:
            self.df = self.df[cols]
        return self.df

    def read_data_from_csv(self, file_path, cols = None):
        self.df = pd.read_csv(file_path, header=0, index_col=False)
        if cols is not None:
            self.df = self.df[cols]
        return self.df

    def get_raw_data(self):
        print(self.df)
        return self.df

    def get_buliding_data(self):
        print(self.buliding_df)
        return self.buliding_df

    def drop_col(self, cols):
        target_df = self.df.drop(cols, axis=1)
        return target_df

    def load_meters_from_buliding(self, building, sample_rate='1min'):
        """
            extract the target_building's data from the pooling data 
            transform the extracted data to long format
            ___________________________________________
            
            input:           
                building : int, example: 2
            
            return:
                the long format of the target data in pd
                 
        """
        
        df_building = self.df.groupby('buildingid').get_group(building)
        df_building.index = pd.to_datetime(df_building['reporttime'])
        # main
        df_result_long = df_building.groupby('channelid').get_group(0) # 0 is the code for main
        df_result_long = df_result_long.resample(sample_rate, how='mean')
        df_result_long = df_result_long[['w']]
        df_result_long = df_result_long.rename(columns={"w": 'main'})
        # channels except main channel
        for channel in self.find_all_channels(df_building):
            if channel != 0 : # 0 is the code for main
                df_channel = df_building.groupby('channelid').get_group(channel)
                df_channel = df_channel.resample(sample_rate, how='mean')
                df_channel = df_channel[['w']]
                df_channel = df_channel.rename(columns={"w": self.rename(channel)})
                df_result_long = pd.merge(df_channel, df_result_long, right_index=True, left_index=True, how='left')           
        self.buliding_df.setdefault(building, df_result_long)
        return df_result_long

    def find_all_houses(self):
        return np.unique(self.df['buildingid'])

    def find_all_channels(self, df):
        return np.unique(df['channelid'])

    def meters_state_of_buliding(self, building, channels, threshold=20, sample_rate = '1min'):
        """
            the state record of the builing's data
            ______________________________________
            
            input:
                building: int, for example: 9
                channels : a list, for example: ['fridge', 'air conditioner']
            return:
                the state record
        """
        if building not in self.buliding_df.keys():
            meters_state = self.load_meters_from_buliding(building, sample_rate)
        else:
            meters_state = self.buliding_df[building]           
        meters_state = meters_state[[ meter for meter in channels if meter in meters_state.keys()]] > threshold
        return meters_state

    def select_target_data(self, buildings, target_meters=None, threshold=20, sample_rate = '1min'):
        """
            ______________________________________
            
            input:
                building: a list, for example:[4,5]
                channels : a list, for example: ['fridge', 'air conditioner']
        """
        buildings_meters_state = {}
        
        for target_building in buildings:
            self.load_meters_from_buliding(target_building, sample_rate)            
            meters_state = self.meters_state_of_buliding(target_building, target_meters, threshold, sample_rate)
            buildings_meters_state.setdefault(target_building, meters_state)
        
        return buildings_meters_state

    def data_preprocess(self, buildings, target_meters=['main'], threshold=20, sample_rate='1min', file_name='preprocessed'):
        """
            ______________________________________
            
            input:
                building: a list, for example:[4,5]
              
        """
        buildings_meters_state = self.select_target_data(buildings, target_meters, threshold, sample_rate)
        for building in buildings:
            print 'preprocessed_building_{}.csv saved'.format(building)
            self.save_csv_file(self.buliding_df[building], file_name + '_building_' + str(building))
            self.save_csv_file(buildings_meters_state[building], file_name + '_meters_of_' + str(building))        
        return buildings_meters_state

    def save_csv_file(self, df, file_name):
        if not os.path.isdir('data'):
            os.makedirs('data')       
        csv_path = os.path.join('data', file_name + '.csv')
        print '{}.csv saved'.format(file_name) 
        df.to_csv(csv_path)

    def extract_switch_moment(self, buildings, channels, threshold=20, sample_rate='60min'):
        """
            extract the switch moment
            ____________________________
            
            input:
                buildings : a list, example: [4,5] or [9]
                channels : a list, example:  ['television', 'fridge', 'air conditioner']
                threshold : the threshold to define on or off
                
             return:
                a building switch table                
        """
        buildings_meters_state = self.data_preprocess(buildings, channels, threshold, sample_rate)
        
        for building in buildings:
            switch_table = {}
            for channel in channels:
                if channel not in buildings_meters_state[building].keys():
                    continue

                timestamps=[buildings_meters_state[building][channel].ne(False).idxmax()]
                final_timestamps = buildings_meters_state[building].index[-1]
                while(timestamps[-1] < final_timestamps):
                    if(buildings_meters_state[building][channel][timestamps[-1]]):
                        timestamps.append(buildings_meters_state[building][channel][timestamps[-1]:].ne(True).idxmax())
                    else:
                        timestamps.append(buildings_meters_state[building][channel][timestamps[-1]:].ne(False).idxmax())

                    if timestamps[-1] == timestamps[-2]:
                        timestamps.pop()
                        break

                meter_state = buildings_meters_state[building][channel][timestamps]                
                switch_table.setdefault(channel, meter_state)
                
            self.building_switch.setdefault(building, switch_table)
            
        return self.building_switch

    def concate_appliances_state(self):
        for building in self.building_switch.keys():
            union_time_stamps = pd.Index([])
            
            for appliance in self.building_switch[building].keys():
                union_time_stamps = union_time_stamps.union(self.building_switch[building][appliance].index)
                
            building_representation = pd.DataFrame(None, index = union_time_stamps)
            for appliance in self.building_switch[building].keys():
                building_representation[appliance] = self.building_switch[building][appliance]

            self.building_representation.setdefault(building, building_representation)                
        return self.building_representation

    def get_usage_representation(self, buildings, channels, threshold=20, sample_rate='60min'):
        """
            extract the switch moment
            ____________________________
            
            input:
                buildings : a list, example: [4,5] or [9]
                channels : a list, example:  ['television', 'fridge', 'air conditioner']
                
             return:
                final step              
        """
        file_name = 'usage_representation'
        self.data_preprocess(buildings, channels, threshold, sample_rate, file_name)
        self.extract_switch_moment(buildings, channels, threshold, sample_rate)
        self.concate_appliances_state()
        self.usage_representation = {}
        
        for building in self.building_representation.keys():
            building_data = self.building_representation[building]
            appliances = building_data.keys()
            encode = {True:1, False:-1}
            representation_index = building_data.index
            representation_data = [[encode[building_data[app][idx]] * self.appliance_in_code(app)
                                    for app in appliances if not math.isnan(building_data[app][idx])]
                                   for idx in representation_index]
            usage_representation = pd.Series(representation_data, index=representation_index)
            self.usage_representation.setdefault(building, usage_representation)                     
        return self.usage_representation
    
    def rename(self, name):
        if name == 0:
            name = 'main'
        elif name == 1:
            name = 'others'
        elif name == 2:
              name = 'television'
        elif name == 3:
              name = 'fridge'
        elif name == 4:
              name = 'air conditioner'
        elif name == 5:
              name = 'bottle warmer'
        elif name == 6:
              name = 'washing machine'
        elif name == '1002':
              name = 'fridge'
        elif name == '1004':
              name = 'air conditioner'        
        return name
    
    def appliance_in_code(self, name):
        if name == 'main':
            name = 0
        elif name == 'television':
              name = 2
        elif name == 'fridge':
              name = 3
        elif name == 'air conditioner':
              name = 4
        elif name == 'bottle warmer':
              name = 5
        elif name == 'washing machine':
              name = 6
        elif name == 'fridge':
              name = 1002
        elif name == 'air conditioner' :
              name = 1004        
        return float(name)
