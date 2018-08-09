#!/usr/bin/python
# -*- coding: utf-8 -*-
import cPickle
from time import strftime
import pandas as pd
import numpy as np
from sql_data import sql4data
from sql_process import cluster4sql
from apyori import apriori
import os
import math
from datetime import timedelta

class data4cluster():
    def __init__(self):
        self.meter_name = {"main":[0], "others":[1], "television":[2], "fridge":[3, 1002], "air conditioner":[4, 1004], "bottle warmer":[5], "washing machine":[6]}
        self.appliance_code = {2:"television", 3:"fridge", 4:"air conditioner", 5:"bottle warmer", 6:"washing machine"}
        self.buliding_df = {}
        self.building_switch = {}
        self.building_representation = {}
        self.df = None
        
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

    def decode2meter(self, result):
        """
        Decode the result from apriori algorithm with meters name.
        return the list of string.
        
        """
        if result is None:
            print('None type result...')
            return
        
        result_msg = []    
        for record in result:
            msg = str(record)
            for app_code, app_name in self.appliance_code.iteritems():
                app_code = str(app_code) 
                msg = msg.replace(app_code + ']', app_name + ']')
                
            msg = msg.replace('[-', '[off_')             
            result_msg.append(msg)
            print(msg)

        return result_msg
        
    def load_meters_from_buliding(self, target_building, meters_name=[], sample_rate = '1min'):
        """
        Extract target_building's meters from the raw data,
        returning the pd.dataframe which is merged with the 'left join'.

        Parameters
        ----------
        target_building : int, the required building.
        meters_name : list, the required meters for the target building.
        sample_rate : Resampling the recorded data, the method is set to be 'mean'.
    
        """
        if self.df is None:
            self.read_data_from_csv()
            
        if len(meters_name) < 1 :
            meters_name = self.meter_name.keys()

        if 'main' in meters_name:
            meters_name.remove('main')
            
        building_meters = self.df.groupby('buildingid').get_group(target_building)
        building_meters.index = pd.to_datetime(building_meters['reporttime'])
        building_meters = building_meters.groupby('channelid')
        building_channels = building_meters.groups.keys()
        
        if self.meter_name['main'][0] not in building_channels: return
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
        if buliding_df is not None:
            self.buliding_df.setdefault(target_building, buliding_df)
        
        return buliding_df

    def find_all_houses(self):
        return np.unique(self.df['buildingid'])

    def find_all_channels(self, df):
        return np.unique(df['channelid'])

    def meters_state_of_buliding(self, target_building, target_appliances, threshold=20):
        """
        Extract meters state from the preprocessed data for each building,
        returning the pd.dataframe which recorded the meter state for each recorded timestamp.

        Parameters
        ----------
        target_building : int, the required building, if it exists in raw_data.
        target_appliances : list, the required appliances for the target building.
        threshold : If the recorded power usage is above this value, it would be determined to be 'On state'.
    
        """
        if target_building not in self.buliding_df.keys():
            return
        
        meters_state = self.buliding_df[target_building]        
        meters_state = meters_state[[ meter for meter in target_appliances if meter in meters_state.keys()]] > threshold
        return meters_state

    def select_target_data(self, buildings, target_meters=None, threshold=20, sample_rate = '1min'):
        """
        Preprocess the target building with the member function with load_meters_from_bulidin() and meters_state_of_buliding(),
        preparing the meters state for the representation series calculations.
    
        input:
            building: list, for example:[4,5]
            target_meters : list, for example: ['fridge', 'air conditioner']
    
        """
        buildings_meters_state = {}
        
        for target_building in buildings:
            self.load_meters_from_buliding(target_building, target_meters, sample_rate)
            
            meters_state = self.meters_state_of_buliding(target_building, target_meters, threshold)
            if meters_state is not None:
                buildings_meters_state.setdefault(target_building, meters_state)
        
        return buildings_meters_state

    def data_preprocess(self, buildings, target_meters=None, threshold=20, sample_rate='1min'):
        buildings_meters_state = self.select_target_data(buildings, target_meters, threshold, sample_rate)
        self.check_parameters(buildings)

        self.save_dict2csv(self.buliding_df, file_name='buliding_df')
        self.save_dict2csv(buildings_meters_state, file_name='meters_state')
        
        return buildings_meters_state

    def save_dict2csv(self, df, file_name):
        message = 'The .csv of ' + file_name + '_result is saved'
        for target_building in df.keys():
            if df[target_building] is None:
                continue
            self.save_csv_file(df[target_building], file_name + '_building_' + str(target_building),  message)

    def save_csv_file(self, df, file_name, message='The .csv of result data is saved'):
        if not os.path.isdir('result'):
            os.makedirs('result')
        
        csv_path = './result/' + file_name + '.csv'
        df.to_csv(csv_path)
        print (message)

    def extract_switch_moment(self, buildings, target_meters, num_on_state=6, threshold=20, sample_rate='60min'):
        """
        Extract the switch moment at each timestamp.

        Parameters
        ----------
        buildings : list.
        target_meters : list.
        num_on_state : The num of 'On state' during each sampling.
    
        """
        buildings_meters_state = self.data_preprocess(buildings, target_meters, threshold)
        buildings_meters_state = self.check_meters_state(buildings_meters_state, target_meters, num_on_state, sample_rate)
        
        for building in buildings:
            switch_table = {}
            for appliance in target_meters:
                if appliance not in buildings_meters_state[building].keys():
                    continue

                timestamps=[buildings_meters_state[building][appliance].ne(False).idxmax()]
                if not buildings_meters_state[building][appliance][timestamps[-1]]:
                    continue
                    
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
                
            switch_df = pd.DataFrame.from_dict(switch_table)
            if switch_df is not None:
                self.building_switch.setdefault(building, switch_df)

        self.save_dict2csv(self.building_switch, file_name='swich_moment')       
        return self.building_switch

    def check_meters_state(self, buildings_meters_state, target_meters, num_on_state, sample_rate, continuous_timestamps='3min'):
        """
        Check the meter state is continuous, the defalut value is set to be 3min, and return the meters state of each sampling duration.
        The 'On state' refers the On states in sampling duration is above the num_on_states.

        Parameters
        ----------
        buildings_meters_state : pd.dataframe, which records the on_off state of meters at each timestamp.
        target_meters : list.
        num_on_states : int, The num of 'On state' during each sampling.
        continuous_points : Appliance is determined to be 'on state', if the power usage above the threshold in the contious timestamps
    
        """
        for building in buildings_meters_state.keys():
            buildings_meters_state[building] = buildings_meters_state[building].resample(continuous_timestamps, how='min')
            buildings_meters_state[building] = buildings_meters_state[building].fillna(0)
            buildings_meters_state[building] = buildings_meters_state[building].resample(sample_rate, how='sum')
            buildings_meters_state[building] = buildings_meters_state[building][[ meter for meter in target_meters if meter in buildings_meters_state[building].keys()]] > num_on_state/3
        return buildings_meters_state

    def concate_appliances_state(self):
        for building in self.building_switch.keys():
            union_time_stamps = pd.Index([])
            
            for appliance in self.building_switch[building].keys():
                union_time_stamps = union_time_stamps.union(self.building_switch[building][appliance].index)
                
            building_representation = pd.DataFrame(None, index = union_time_stamps)
            for appliance in self.building_switch[building].keys():
                building_representation[appliance] = self.building_switch[building][appliance]

            if building_representation is not None:
                self.building_representation.setdefault(building, building_representation)

        self.save_dict2csv(self.building_representation, file_name='concate_states') 
        return self.building_representation

    def get_usage_representation(self, buildings = None, target_meters = None, num_on_state=6, threshold=20, sample_rate='60min'):
        """
        Executing the preprocess algorithms for the raw data, returning the usage representationin pd.Series.

        Parameters
        ----------
        num_on_state : int, The num of 'On state' during each sampling.
    
        """
        
        if buildings is None or target_meters is None:
            buildings, target_meters = self.init_parameters(buildings, target_meters)
                    
        self.extract_switch_moment(buildings, target_meters, num_on_state, threshold, sample_rate)
        self.concate_appliances_state()
        self.usage_representation = {}
        
        for building in self.building_representation.keys():
            building_data = self.building_representation[building]

            appliances = building_data.keys()
            encode = {True:1, False:-1}
            representation_index = building_data.index
            representation_data = [[encode[building_data[app][idx]] * self.meter_name[app][0]
                                    for app in appliances if not math.isnan(building_data[app][idx])]
                                   for idx in representation_index]
            
            usage_representation = pd.Series(representation_data, index=representation_index)

            if usage_representation is not None:
                self.usage_representation.setdefault(building, usage_representation)

        self.save_dict2csv(self.usage_representation, file_name='usage_representation')
        return self.usage_representation

    def init_parameters(self, buildings, target_meters):
        """
        If input_parameter is None, initializing parameters to compute all the buildings and meters.
    
        """
        if self.df is None:
            self.read_data_from_csv()

        if buildings is None:
            buildings = self.find_all_houses().tolist()

        if target_meters is None:
            target_meters = self.meter_name.keys()
            
        return buildings, target_meters

    def check_parameters(self, buildings):
        for building in buildings:
            if building not in self.buliding_df.keys():
                buildings.remove(building)

    def prepare_apriori_series(self, representation_series):
        start_time_idx = representation_series.index[0].strftime("%Y/%m/%d")
        end_time_idx = (representation_series.index[-1] + timedelta(days=1)).strftime("%Y/%m/%d")
        time_index = pd.date_range(start=start_time_idx, end=end_time_idx, closed='left', freq='H')
        
        apriori_series = pd.Series(index=time_index)
        apriori_series = pd.concat([apriori_series, representation_series], axis=1)
        apriori_series = apriori_series[1]
        apriori_series = apriori_series.fillna("")
        
        return apriori_series

    def execute_apriori(self, representation_series, min_series_len, min_supp = 0.003, min_confi=0.1):
        if representation_series.size < min_series_len:
            print('representation_series is too short...')
            return
        apriori_series = self.prepare_apriori_series(representation_series)
        results = apriori(apriori_series, min_support=min_supp, min_confidence=min_confi)
        return list(results)
        

class ClusterPipeline():
    """
    Class for executing the cluster algorithms including data preproces and SQL storage result.

    Parameters
    ----------
    min_supp : Value for min_support in Apriori algorithm.
    min_confi : Value for min_confident in Apriori algorithm.
    min_len : Value for min_length in Apriori algorithm.

    file_name : The file_name of the raw data.
    buildings, target_meters : list, if both the values is set to be None, the whole buildings and meters will be preprocessed

    Mumber functions
    ----------
    start_data_preprocess : The data preprocess algorithms for representation series.  
    start_sql_storage : Uploading the representation series to the MSSQL.
    demo_algorithms : Presenting the apriori algorithm.
    
    """
    def __init__(self, file_name = 'raw_data', buildings = None, target_meters = None, sample_rate='60min', min_series_len = 10):
        self.data_process = data4cluster()
        self.sql_process = cluster4sql()
        self.raw_data = file_name
        self.buildings = buildings
        self.target_meters = target_meters
        self.sample_rate = sample_rate
        self.min_series_len = min_series_len

    def start_data_preprocess(self, num_on_state=6, threshold=20):
        self.data_process.read_data_from_csv(self.raw_data)
        usage_representation = self.data_process.get_usage_representation(self.buildings, self.target_meters, num_on_state, threshold, self.sample_rate)
        return usage_representation

    def start_sql_storage(self, usage_representation):
        temp_app_loc = (1, 1, 1)
        self.sql_process.result2db(usage_representation, temp_app_loc, self.min_series_len)

    def demo_algorithms(self, target_building = 2, min_supp=0.003, min_confi=0.1):
        usage_representation = self.start_data_preprocess()
        self.start_sql_storage(usage_representation)
        apriori_result = self.data_process.execute_apriori(usage_representation[target_building], self.min_series_len, min_supp, min_confi)
        return apriori_result
