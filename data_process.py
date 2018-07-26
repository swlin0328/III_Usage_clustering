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
        self.meter_name = {"main":[0], "others":[1], "television":[2], "fridge":[3, 1002], "air conditioner":[4, 1004], "bottle warmer":[5], "washing machine":[6]}
        self.buliding_df = {}
        self.building_switch = {}
        self.building_representation = {}
        
    def read_data_from_sql(self, start_date='2017-08-01', end_date='2018-07-31', file_name='raw_data', cols = None):
        sql_db = sql4data()
        self.df = sql_db.read_data(start_date, end_date, file_name)
        if cols is not None:
            self.df = self.df[cols]
        return self.df

    def read_data_from_csv(self, file_name='raw_data', cols = None):
        self.df = pd.read_csv('./data/' + file_name + '.csv', header=0)
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

    def load_meters_from_buliding(self, target_building, meters_name=[], sample_rate = '1min'):
        if len(meters_name) < 1 :
            meters_name = self.meter_name.keys()

        if 'main' in meters_name:
            meters_name.remove('main')
            
        building_meters = self.df.groupby('buildingid').get_group(target_building)
        building_meters.index = pd.to_datetime(building_meters['reporttime'])
        building_meters = building_meters.groupby('channelid')
        building_channels = building_meters.groups.keys()
        
        buliding_df = building_meters.get_group(self.meter_name['main'][0]).rename(columns={"w": "main"})
        buliding_df = buliding_df.resample(sample_rate, how='mean')
        target_meters = ['main']

        for meter, channel_ids in self.meter_name.iteritems():
            if meter in meters_name and channel_ids[0] in building_channels:
                appliance_meter = building_meters.get_group(channel_ids[0]).rename(columns={"w": meter})
                
                for channel_id in channel_ids[1:]:
                    if channel_id not in building_channels: continue
                    another_channel = building_meters.get_group(channel_id).rename(columns={"w": meter})
                    appliance_meter.append(another_channel)

                appliance_meter = appliance_meter.resample(sample_rate, how='mean')
                buliding_df = pd.merge(buliding_df, appliance_meter, right_index=True, left_index=True, how='left')
                target_meters.append(meter)
    
        buliding_df = buliding_df[target_meters]
        buliding_df = buliding_df[~buliding_df.index.duplicated()]
        self.buliding_df.setdefault(target_building, buliding_df)
        
        return buliding_df

    def find_all_houses(self):
        return np.unique(self.df['buildingid'])

    def find_all_channels(self, df):
        return np.unique(df['channelid'])

    def meters_state_of_buliding(self, target_building, target_appliances, threshold=20, sample_rate = '1min'):
        if target_building not in self.buliding_df.keys():
            meters_state = self.load_meters_from_buliding(target_building, target_appliances, sample_rate)
        else:
            meters_state = self.buliding_df[target_building]
            
        meters_state = meters_state[[ meter for meter in target_appliances if meter in meters_state.keys()]] > threshold
        return meters_state

    def select_target_data(self, buildings, target_meters=None, threshold=20, sample_rate = '1min'):
        buildings_meters_state = {}
        
        for target_building in buildings:
            self.load_meters_from_buliding(target_building, target_meters, sample_rate)
            
            meters_state = self.meters_state_of_buliding(target_building, target_meters, threshold, sample_rate)
            buildings_meters_state.setdefault(target_building, meters_state)
        
        return buildings_meters_state

    def data_preprocess(self, buildings, target_meters=['main'], threshold=20, sample_rate='1min', file_name='preprocessed'):
        buildings_meters_state = self.select_target_data(buildings, target_meters, threshold, sample_rate)

        for target_building in buildings:
            message = 'The .csv of preprocessed data is saved'
            self.save_csv_file(self.buliding_df[target_building], file_name + '_building_' + str(target_building),  message)
            self.save_csv_file(buildings_meters_state[target_building], file_name + '_meters_of_' + str(target_building),  message)
        
        return buildings_meters_state

    def save_csv_file(self, df, file_name, message='The .csv of result data is saved'):
        if not os.path.isdir('data'):
            os.makedirs('data')
        
        csv_path = './data/' + file_name + '.csv'
        df.to_csv(csv_path)
        print (message)

    def get_usage_representation(self, buildings, target_meters, threshold=20, sample_rate='60min'):
        file_name = 'usage_representation'
        self.data_preprocess(buildings, target_meters, threshold, sample_rate, file_name)
        
        return self.usage_representation

    def extract_switch_moment(self, buildings, target_meters, threshold=20, sample_rate='60min'):
        buildings_meters_state = self.data_preprocess(buildings, target_meters, threshold, sample_rate)
        
        for building in buildings:
            switch_table = {}
            for appliance in target_meters:
                if appliance not in buildings_meters_state[building].keys():
                    continue

                timestamps=[buildings_meters_state[building][appliance].ne(False).idxmax()]
                final_timestamps = buildings_meters_state[building].index[-1]
                while(timestamps[-1] < final_timestamps):
                    if(buildings_meters_state[building][appliance][timestamps[-1]]):
                        timestamps.append(buildings_meters_state[building][appliance][timestamps[-1]:].ne(True).idxmax())
                    else:
                        timestamps.append(buildings_meters_state[building][appliance][timestamps[-1]:].ne(False).idxmax())

                    if timestamps[-1] == timestamps[-2]:
                        timestamps.pop()
                        break

                meter_state = buildings_meters_state[building][appliance][timestamps]                
                switch_table.setdefault(appliance, meter_state)
                
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

    def get_usage_representation(self):
        for building in self.building_representation.keys():
            building_data = self.building_representation[building]

            appliances = building_data.keys()
            encode = {True:1, False:-1}
            representation_index = building_data.index
            representation_data = [[encode[building_data[app][idx]] * self.meter_name[app][0]
                                    for app in appliances if not math.isnan(building_data[app][idx])]
                                   for idx in representation_index]
            
            usage_representation = pd.Series(representation_data, index=representation_index)
            self.usage_representation.setdefault(building, usage_representation)
                     
        return self.usage_representation
