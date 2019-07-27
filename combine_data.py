#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import sqlite3

from scipy.stats import linregress
import matplotlib.pyplot as plt
from statsmodels.nonparametric.smoothers_lowess import lowess
from pykalman import KalmanFilter
import numpy as np


# In[2]:


def get_weather_data(weather_station_id):
    weather_data = pd.read_csv('weather_data_test/{weather_id}.csv'.format(weather_id=weather_station_id)).set_index('Date/Time')
    rename_map = {'Total Rain (mm)': 'rain', # in mm
                  'Total Precip (mm)': 'precip', # in mm
                  'Max Temp (°C)': 'max_temp', # in C,
                  'Min Temp (°C)': 'min_temp', # in C,
                  'Mean Temp (°C)': 'mean_temp', # in C,
                 }
    weather_data.rename(columns=rename_map, inplace=True)
    weather_data['snow_on_grnd'] = weather_data['Snow on Grnd (cm)'] * 10 # convert to mm
    weather_data['snow_precip'] = weather_data['Total Snow (cm)'] * 10 # convert to mm

    del weather_data['Snow on Grnd (cm)']
    del weather_data['Total Snow (cm)']
    # get rid of weather data values w/o precip data?
    return weather_data#.dropna(subset=['precip'])


# In[15]:


def get_combined_data(daily_data, hydro_station_id, weather_station_id):
    LEVELS = ['LEVEL{}'.format(dayno) for dayno in range(1,32)]
    station_day_data = daily_data[daily_data['STATION_NUMBER'] == hydro_station_id]
    melted_day_data = station_day_data.melt(id_vars = ['YEAR', 'MONTH'], value_vars=LEVELS).dropna()
    melted_day_data['date'] = pd.to_datetime(melted_day_data['YEAR'].map(str) + '-' + melted_day_data['MONTH'].map(str)+ '-' + melted_day_data['variable'].str[5:])
    melted_day_data.set_index('date', inplace=True)

    weather_data = get_weather_data(weather_station_id)

    merged_data = pd.merge(melted_day_data, weather_data, left_index=True, right_index=True)
    del merged_data['variable']
    hdf = merged_data.rename(columns={'value': 'water_level', 'YEAR': 'year', 'MONTH': 'month'})
    hdf.to_hdf('combined_data.hdf', 'hydro_{}'.format(hydro_station_id), mode='a')


# In[16]:


def get_station_data(station):
    return get_combined_data(daily_data, station['hydro_id'], station['weather_station_id'])


# In[17]:


rename_map = {'HYDRO_ID': 'hydro_id',
              'Latitude (Decimal Degrees)': 'latitude',
              'Longitude (Decimal Degrees)': 'longitude',
              'Name': 'weather_name',
              'Climate ID': 'weather_climate_id',
              'Station ID': 'weather_station_id',
              'First Year': 'weather_first_year',
              'Last Year': 'weather_last_year',
              'Elevation (m)': 'weather_elevation(m)', # in m
              'First Year': 'weather_first_year',
              'Last Year': 'weather_last_year',
             }
station_data = pd.read_csv('reservoir_weather_data.csv')
station_data.rename(columns=rename_map, inplace=True)
#'HYDRO_ID' for hydro station id 'Station ID' for weather station id
#station_data


# In[18]:


# see http://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/HYDAT_Definition_EN.pdf for HYDAT schema
db_filename = 'Hydat.sqlite3'
conn = sqlite3.connect(db_filename)


# In[19]:


station_filter_str = str(list(station_data['hydro_id']))[1:-1]
daily_levels_query = "SELECT * FROM DLY_LEVELS WHERE STATION_NUMBER IN ({stations})".format(stations=station_filter_str)
daily_data = pd.read_sql_query(daily_levels_query,conn)


# In[20]:


#station_data.apply(get_station_data, axis=1)
get_station_data(station_data.iloc[0])


# In[ ]:





# In[ ]:




