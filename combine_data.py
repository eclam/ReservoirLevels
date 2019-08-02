import numpy as np
import pandas as pd
import sqlite3

#in general, 'HYDRO_ID' for hydro station id 'Station ID' for weather station id

DAILY_WEATHER_DIR = './data/consolidated_weather_data/{weather_id}-daily.csv'
MONTHLY_WEATHER_DIR = './data/consolidated_weather_data/{weather_id}-monthly.csv'
HYDAT_DB = './data/Hydat.sqlite3'
MONTHLY_HDF_OUTPUT = 'monthly_combined_data.hdf'
DAILY_HDF_OUTPUT = 'daily_combined_data.hdf'

station_data = pd.read_csv('./index_data/closest_weather_to_hydro_stations.csv')
rename_map = { station_data.columns[0]: 'hydro_id',
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
station_data = station_data.rename(columns=rename_map)
station_filter_str = str(list(station_data['hydro_id']))[1:-1]

# see http://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/HYDAT_Definition_EN.pdf for HYDAT schema
conn = sqlite3.connect(HYDAT_DB)
daily_levels_query = "SELECT * FROM DLY_LEVELS WHERE STATION_NUMBER IN ({stations})".format(stations=station_filter_str)
daily_data = pd.read_sql_query(daily_levels_query,conn)


def get_daily_weather_data(weather_station_id):
    # get daily weather data from a csv
    weather_data = pd.read_csv(DAILY_WEATHER_DIR.format(weather_id=weather_station_id)).set_index('Date/Time')
    rename_map = {'Total Rain (mm)': 'rain', # in mm
                  'Total Precip (mm)': 'precip', # in mm
                  'Max Temp (°C)': 'max_temp', # in C,
                  'Min Temp (°C)': 'min_temp', # in C,
                  'Mean Temp (°C)': 'mean_temp', # in C,
                  'Snow on Grnd (cm)': 'snow_on_grnd',
                  'Total Snow (cm)': 'snow_precip',
                  'Day': 'day'
                 }
    weather_data.rename(columns=rename_map, inplace=True)
    weather_data['snow_on_grnd'] = weather_data['snow_on_grnd'] * 10 # convert to mm
    weather_data['snow_precip'] = weather_data['snow_precip'] * 10 # convert to mm

    return weather_data


def get_monthly_weather_data(weather_station_id):
    # get monthly waether data from a csv
    weather_data = pd.read_csv(MONTHLY_WEATHER_DIR.format(weather_id=weather_station_id)).set_index('Date/Time')
    rename_map = {'Mean Max Temp (°C)':'mean_max_temp', # in C
                'Mean Min Temp (°C)':'mean_min_temp', # in C
                'Mean Temp (°C)':'mean_temp',         # in C
                'Extr Max Temp (°C)':'month_max_temp',# in C
                'Extr Min Temp (°C)':'month_min_temp',# in C
                'Total Rain (mm)':'rain',             # in mm
                'Total Precip (mm)':'precip',         # in mm
                'Total Snow (cm)':'total_snow',       # in mm
                'Snow Grnd Last Day (cm)':'remaining_snow',
                }
    weather_data.rename(columns=rename_map, inplace=True)
    weather_data['total_snow'] = weather_data['total_snow']*10
    weather_data['remaining_snow'] = weather_data['remaining_snow']*10

    return weather_data#.dropna(subset=['precip'])


def get_daily_combined_data(hydro_station_id, weather_station_id):
    # get daily hydro data and combine it with daily weather data
    LEVELS = ['LEVEL{}'.format(dayno) for dayno in range(1,32)]
    station_day_data = daily_data[daily_data['STATION_NUMBER'] == hydro_station_id]

    melted_day_data = station_day_data.melt(id_vars = ['YEAR', 'MONTH'], value_vars=LEVELS).dropna()
    melted_day_data['date'] = pd.to_datetime(melted_day_data['YEAR'].map(str) + '-' + melted_day_data['MONTH'].map(str)+ '-' + melted_day_data['variable'].str[5:])
    melted_day_data.set_index('date', inplace=True)

    weather_data = get_daily_weather_data(weather_station_id)

    merged_data = pd.merge(melted_day_data, weather_data, left_index=True, right_index=True)
    del merged_data['variable']
    del merged_data['Year']
    del merged_data['Month']
    df = merged_data.rename(columns={'value': 'water_level', 'YEAR': 'year', 'MONTH': 'month'})
    df.to_hdf(DAILY_HDF_OUTPUT, key='hydro_{}'.format(hydro_station_id), mode='a')
    return df


def get_monthly_combined_data(hydro_station_id, weather_station_id):
    # get monthly hydro data and combine it with daily weather data
    station_day_data = daily_data[daily_data['STATION_NUMBER'] == hydro_station_id]
    
    monthly_hydro_rename_map = {
        'YEAR' : 'year', 
        'MONTH': 'month',
        'MONTHLY_MEAN': 'monthly_water_mean',
        'MONTHLY_TOTAL': 'monthly_water_total',
        'MIN': 'monthly_water_min',
        'MAX': 'monthly_water_max',
        'FIRST_DAY_MIN': 'min_water_day',
        'FIRST_DAY_MAX': 'max_water_day'
    }

    monthly_data = station_day_data[list(monthly_hydro_rename_map.keys())].dropna().rename(columns=monthly_hydro_rename_map)
    monthly_data['year_month'] = monthly_data['year'].map(str) + '-' + monthly_data['month'].map(lambda x: str(x).zfill(2))
    monthly_data.set_index('year_month', inplace=True)
    
    weather_data = get_monthly_weather_data(weather_station_id)
    
    merged_data = pd.merge(monthly_data, weather_data, left_index=True, right_index=True)

    del merged_data['Year']
    del merged_data['Month']
    merged_data.to_hdf(MONTHLY_HDF_OUTPUT, key='hydro_{}'.format(hydro_station_id), mode='a')
    return merged_data
    
    
def get_station_data(station):
    # combine the hydro and weather data for this station and save it to an hdf
    get_daily_combined_data(station['hydro_id'], station['weather_station_id'])
    get_monthly_combined_data(station['hydro_id'], station['weather_station_id'])


station_data.apply(get_station_data, axis=1)

