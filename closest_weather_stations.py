#!/usr/bin/env python
# coding: utf-8

import sys
import sqlite3
import numpy as np
import pandas as pd


def distance(hydro_data, weather_stations):
    """
    Given a hydro_data with 'latitude'/'longitude' (degrees)
    calculate its distance from each station 'lat'/'lon' (radians)
    """
    def hav(theta):
        """
        Haversine function on an angle (in radians)
        """
        return 0.5 * (1 - np.cos(theta))

    hydro_data_lat = np.radians(hydro_data['LATITUDE'])
    hydro_data_lon = np.radians(hydro_data['LONGITUDE'])

    r = 6371  # in km
    h1 = hav(weather_stations['LAT_RAD'] - hydro_data_lat)
    h2 = np.cos(hydro_data_lat) * \
        np.cos(weather_stations['LAT_RAD'])*hav(weather_stations['LON_RAD']-hydro_data_lon)

    return 2 * r * np.arcsin(np.sqrt(h1 + h2))


def closest_station(hydro_data, weather_stations):
    """
        Returns the tmax of the station closest to the hydro_data
    """
    # get a single station for consistency
    first_year = hydro_data['YEAR_FROM']
    last_year = hydro_data['YEAR_TO']
    weather_stations = weather_stations[(weather_stations['First Year'] <= first_year)
                        & (weather_stations['Last Year'] >= last_year)]
    if len(weather_stations) <= 0:
        # no suitable stations found
        return None
    distances = distance(hydro_data, weather_stations)

    return_row = weather_stations.loc[distances.idxmin()][:]
    return_row['DISTANCE'] = distances.min()
    return_row['HYDRO_ID'] = hydro_data['STATION_NUMBER']
    return return_row


# see http://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/HYDAT_Definition_EN.pdf for HYDAT schema
hydro_data = pd.read_csv('./index_data/filtered_station_inventory.csv')
hydro_data['LAT_RAD'] = np.radians(hydro_data['LATITUDE'])
hydro_data['LON_RAD'] = np.radians(hydro_data['LONGITUDE'])

weather_data = pd.read_csv("./index_data/filteredNULL_weather_inventory.csv", sep=',')
weather_data['LAT_RAD'] = np.radians(weather_data['Latitude (Decimal Degrees)'])
weather_data['LON_RAD'] = np.radians(weather_data['Longitude (Decimal Degrees)'])

weather_data = weather_data[['Latitude (Decimal Degrees)', 'Longitude (Decimal Degrees)', 'LAT_RAD',
                             'LON_RAD', 'Name', 'Station ID', 'First Year', 'Last Year',
                             "daily_temp_nullcount", "monthly_temp_nullcount",
                             "daily_precipiation_nullcount","monthly_precipiation_nullcount",
                             "daily_snow_nullcount","monthly_snow_nullcount"]]

# link up the best (closest station w/data) weather to hydro station
best_weather_stations = hydro_data.apply(closest_station, axis=1, weather_stations=weather_data)\
                                            .set_index('HYDRO_ID')

hydro_data = hydro_data.drop(['LAT_RAD','LON_RAD'],axis=1)
weather_data = weather_data.drop(['LAT_RAD','LON_RAD'],axis=1)
best_weather_stations = best_weather_stations.drop(['LAT_RAD','LON_RAD'],axis=1)

weather_data = pd.merge(best_weather_stations, hydro_data.set_index('STATION_NUMBER')
                        ,left_index=True, right_index=True)

weather_data = weather_data.drop_duplicates().drop(['Unnamed: 0'],axis=1)

# Dropping stations that are WAY too far away to even have an accurate reading 
# weather_data = weather_data[weather_data['DISTANCE']<50]

weather_data.to_csv('./index_data/closest_weather_to_hydro_stations.csv')
