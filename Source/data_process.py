#!/usr/bin/python
# -*- coding: utf-8 -*-
import cPickle
from time import strftime
import pandas as pd
import numpy as np
from sql_data import sql4data
from sql_process import correlation4sql
from apyori import apriori
import os
import math
from datetime import timedelta
from sqlalchemy import create_engine
import pymssql
from tqdm import tqdm
import re

class data4correlation():
    def __init__(self, dataset='@@@'):
        self.meter_name = {"main":[0], "others":[1], "television":[2], "fridge":[3, 1002], "air conditioner":[4, 1004], "bottle warmer":[5], "washing machine":[6]}
        if dataset == 'uk':
            self.meter_name = {"main": [26], "vacuum_cleaner": [25], "washer_dryer": [24], "microwave": [23],
                           "dishwasher": [22], "electric_hob": [21], "oven": [20],
                           "fridge_freezer": [19], "kettle": [18], "sky_hd_box": [17],
                           "home_theatre_amp": [16], "toaster": [15], "atom_pc": [14],
                           "nespresso_pixie": [13], "steam_iron": [12], "PS4": [11],
                           "24_inch_lcd": [10], "core2_server": [9], "network_attached_storage": [8],
                           "treadmill": [7], "24_inch_lcd_bedroom": [6], "primary_tv": [5],
                           "hairdryer": [4], "i7_desktop": [3], "stereo_speakers_bedroom": [2], "aggregate": [1]}

        self.appliance_code = {value[0]:key for key, value in self.meter_name.iteritems()}
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
                msg = msg.replace('[' + app_code, '[' + app_name)
                msg = msg.replace(app_code + ']', app_name + ']')
                msg = msg.replace(' ' + app_code + ',', ' ' + app_name + ',')
                
            msg = msg.replace('[-', '[off_')
            msg = msg.replace(', -', '[off_')
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
        building_meters.index = pd.to_datetime(building_meters['reporttime'], format='%Y-%m-%d %H:%M:%S')
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

    def meters_state_of_buliding(self, target_building, target_appliances, threshold=70):
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

    def select_target_data(self, buildings, target_meters=None, threshold=70, sample_rate = '1min'):
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

    def data_preprocess(self, buildings, target_meters=None, threshold=70, sample_rate='1min'):
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

    def save_csv_file(self, df, file_name, message=None, list_idx=True):
        if not os.path.isdir('result'):
            os.makedirs('result')
        
        csv_path = './result/' + file_name + '.csv'
        df.to_csv(csv_path, index = list_idx)
        if message is not None:
            print (message)

    def extract_switch_moment(self, buildings, target_meters, num_on_state=4, threshold=70, sample_rate='60min'):
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

    def check_meters_state(self, buildings_meters_state, target_meters, num_on_state, sample_rate, continuous_timestamps='4min'):
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
        state_per_duration = int(continuous_timestamps.split('min')[0])
        for building in buildings_meters_state.keys():
            buildings_meters_state[building] = buildings_meters_state[building].resample(continuous_timestamps, how='min')
            buildings_meters_state[building] = buildings_meters_state[building].fillna(False)
            buildings_meters_state[building] = buildings_meters_state[building].resample(sample_rate, how='sum')
            buildings_meters_state[building] = buildings_meters_state[building][[ meter for meter in target_meters if meter in buildings_meters_state[building].keys()]] >= num_on_state/state_per_duration
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

    def get_usage_representation(self, buildings = None, target_meters = None, num_on_state=4, threshold=70, sample_rate='60min'):
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
            encode = {True:"On_", False:"Off_"}
            representation_index = building_data.index
            representation_data = [[encode[building_data[app][idx]] + app
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

    def prepare_apriori_series(self, representation_series, start_date, end_date):
        start_time_idx = representation_series.index[0].strftime("%Y/%m/%d")
        end_time_idx = (representation_series.index[-1] + timedelta(days=1)).strftime("%Y/%m/%d")
        time_index = pd.date_range(start=start_time_idx, end=end_time_idx, closed='left', freq='H')
        
        apriori_series = pd.Series(index=time_index)
        apriori_series = pd.concat([apriori_series, representation_series], axis=1)
        apriori_series = apriori_series[1]
        apriori_series = apriori_series.fillna("")

        if start_date is not None and end_date is not None:
            mask = (apriori_series.index > start_date) & (apriori_series.index <= end_date)
            apriori_series = apriori_series.loc[mask]
        
        return apriori_series

    def execute_apriori(self, representation_series, min_series_len, start_date=None, end_date=None,  window_size='4hour', min_supp=0.003, min_confi=0.1):
        if representation_series.size < min_series_len:
            print('representation_series is too short...')
            return
        if start_date is None:
            representation_series.index[0]
        if end_date is None:
            representation_series.index[-1]

        apriori_result = []
        apriori_series = self.prepare_apriori_series(representation_series, start_date, end_date)
        window_size = int(window_size.split('hour')[0])

        while(start_date < end_date):
            target_time = start_date + pd.Timedelta(hours=window_size-1)
            target_series = apriori_series[start_date:target_time]
            results = apriori(target_series, min_support=min_supp, min_confidence=min_confi)
            apriori_result.append(list(results))
            start_date = target_time + pd.Timedelta(hours=1)

        return apriori_result

    def save_apriori(self, apriori_result, building, sample_rate, duration, window_size, file_name, host='mssql+pymssql://user:password.@@@.@@@.@@\SQLEXPRESS01/db', to_sql=False):
        if apriori_result is None:
            return
        df = pd.DataFrame()
        window_size = int(window_size.split('hour')[0])
        statistic_data = {}

        for group_idx, duration_result in enumerate(apriori_result):
            group_idx = int(group_idx)
            num_group = int(24 / window_size)
            group = group_idx % num_group
            appliance = {}
            correlation = {}
            statistic_data.setdefault(group, appliance)

            for result in duration_result:
                if len(result) < 3:
                    continue
                tmp_data = {}
                app = str(result[0])
                tmp_data.setdefault('day', int(group_idx/num_group)+1)
                tmp_data.setdefault('group', group+1)
                tmp_data.setdefault('building', building)
                tmp_data.setdefault('sample_rate', sample_rate)
                tmp_data.setdefault('appliance', app)
                tmp_data.setdefault('support', round(result[1], 4))
                tmp_data.setdefault('duration', duration[0].strftime("%Y/%m/%d") + ' ~ ' + duration[-1].strftime("%Y/%m/%d"))
                tmp_data.setdefault('correlation_0', '')
                tmp_data.setdefault('correlation_1', '')
                tmp_data.setdefault('correlation_2', '')
                tmp_data.setdefault('correlation_3', '')
                tmp_data.setdefault('correlation_4', '')
                tmp_data.setdefault('correlation_5', '')
                statistic_data[group].setdefault(app, correlation)

                for idx in range(len(result[2])):
                    col = 'correlation_' + str(idx)
                    corr = str(result[2][idx])
                    tmp_data[col] = corr
                    key = corr.split(', confidence')[0] + ')'
                    statistic_data[group][app].setdefault(key, 0)
                    statistic_data[group][app][key] = statistic_data[group][app][key] + 1

                df = df.append(tmp_data, ignore_index=True)
        if df.shape[0] < 2:
            return

        df = df.sort_values('group')
        df.index.rename('item')
        if to_sql:
            self.df2sql(df, table_name='apriori_result', host=host)
        self.save_csv_file(df, file_name, list_idx=False)
        return statistic_data

    def df2sql(self, df, table_name, host, method='append'):
        engine = create_engine(host)
        df.to_sql(name=table_name, con=engine, index=False, if_exists=method)

    def filter_apriori_result(self, results, threshold_result=2):
        for group_idx in range(len(results)):
            for idx in range(len(results[group_idx]), 0, -1):
                idx = idx-1
                if len(results[group_idx][idx][0]) < threshold_result:
                    del results[group_idx][idx]
        return results

    def convert_uk2rawdata(self, file_path='building5/elec/'):
        DATA_DIR = "data/uk/" + file_path
        df = pd.DataFrame(columns=['reporttime', 'w', 'buildingid', 'channelid'])
        for filename in os.listdir(DATA_DIR):
            print("Loading: ", filename)
            house_df = pd.read_csv(DATA_DIR + filename, header=0)
            building_df = pd.DataFrame(columns=['reporttime', 'w', 'buildingid', 'channelid'])
            building_df['reporttime'] = house_df[3:]['physical_quantity']
            building_df['w'] = house_df[3:]['power']
            building_df['channelid'] = filename.split('.')[0].split('meter')[1]
            building_df['buildingid'] = file_path.split('/')[0].split('building')[1]
            df = df.append(building_df, ignore_index=True)

        df.to_csv('data/uk_rawdata.csv', index=False)
        print('Successfully converted...')

class CorrelationPipeline():
    """
    Class for executing the correlation algorithms including data preproces and SQL storage result.

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
    def __init__(self, data_set='III', file_name='raw_data', buildings=None, target_meters=None, min_series_len=3):
        self.data_process = data4correlation(data_set)
        self.sql_process = correlation4sql()
        self.raw_data = file_name
        self.buildings = buildings
        self.target_meters = target_meters
        self.min_series_len = min_series_len

    def start_data_preprocess(self, sample_rate=60, num_on_state=4, threshold=70):
        self.data_process.read_data_from_csv(self.raw_data)
        if self.buildings is None or self.target_meters is None:
            self.buildings, self.target_meters = self.data_process.init_parameters(self.buildings, self.target_meters)
        period = str(sample_rate) + 'min'
        usage_representation = self.data_process.get_usage_representation(self.buildings, self.target_meters, num_on_state, threshold, period)
        return usage_representation

    def start_sql_storage(self, usage_representation):
        temp_app_loc = (1, 1, 1)
        self.sql_process.result2db(usage_representation, temp_app_loc, self.min_series_len)

    def demo_algorithms(self, start_date=None, end_date=None, window_size='4hour', sample_rate=60, min_supp=0.003, min_confi=0.1):
        usage_representation = self.start_data_preprocess(sample_rate)
        self.start_sql_storage(usage_representation)
        if start_date is None:
            representation_series.index[0]
        else:
            start_date = pd.to_datetime(start_date)

        if end_date is None:
            representation_series.index[-1]
        else:
            end_date = pd.to_datetime(end_date)
        duration = [start_date, end_date]

        for target_building in tqdm(self.buildings):
            if usage_representation[target_building].size < 2:
                continue
            apriori_result = self.data_process.execute_apriori(usage_representation[target_building], self.min_series_len, start_date, end_date,  window_size, min_supp, min_confi)
            apriori_result = self.data_process.filter_apriori_result(apriori_result, threshold_result=2)
            statistic = self.data_process.save_apriori(apriori_result, target_building, sample_rate, duration, window_size, file_name = 'apriori_of_building_' + str(target_building) + '_rate_' + str(sample_rate) + '_wsize_' + str(window_size), to_sql=True)
            for group, app in statistic.iteritems():
                for appliance, corr in app.iteritems():
                    for order, count in corr.iteritems():
                        print('The ' + appliance + ' in group ' + str(group) + ' for correlation ' + order + ' = ' + str(count))

        print('All the computations are completed...')
