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


def get_combined_data(daily_data, hydro_station_id, weather_station_id):
    LEVELS = ['LEVEL{}'.format(dayno) for dayno in range(1,32)]
    station_day_data = daily_data[daily_data['STATION_NUMBER'] == hydro_station_id]
    melted_day_data = station_day_data.melt(id_vars = ['YEAR', 'MONTH'], value_vars=LEVELS).dropna()
    melted_day_data['date'] = pd.to_datetime(melted_day_data['YEAR'].map(str) + '-' + melted_day_data['MONTH'].map(str)+ '-' + melted_day_data['variable'].str[5:])
    melted_day_data.set_index('date', inplace=True)

    weather_data = pd.read_csv('weather_data_test/{weather_id}.csv'.format(weather_id=weather_station_id)).set_index('Date/Time')
    return pd.merge(melted_day_data, weather_data, left_index=True, right_index=True)


# In[3]:


station_data = pd.read_csv('reservoir_weather_data.csv')
#'HYDRO_ID' for hydro station id 'Station ID' for weather station id


# In[4]:


# see http://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/HYDAT_Definition_EN.pdf for HYDAT schema
db_filename = 'Hydat.sqlite3'
conn = sqlite3.connect(db_filename)


# In[5]:


station_filter_str = str(list(station_data['HYDRO_ID']))[1:-1]
daily_levels_query = "SELECT * FROM DLY_LEVELS WHERE STATION_NUMBER IN ({stations})".format(stations=station_filter_str)
daily_data = pd.read_sql_query(daily_levels_query,conn)


# In[6]:


get_combined_data(daily_data, '08MH148', 776)

