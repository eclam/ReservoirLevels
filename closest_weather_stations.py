#!/usr/bin/env python
# coding: utf-8

import sys
import sqlite3
import numpy as np
import pandas as pd


def distance(entity, stations):
    """
    Given a entity with 'latitude'/'longitude' (degrees)
    calculate its distance from each station 'lat'/'lon' (radians)
    """
    def hav(theta):
        """
        Haversine function on an angle (in radians)
        """
        return 0.5 * (1 - np.cos(theta))

    entity_lat = np.radians(entity['LATITUDE'])
    entity_lon = np.radians(entity['LONGITUDE'])

    r = 6371  # in km
    h1 = hav(stations['LAT_RAD'] - entity_lat)
    h2 = np.cos(entity_lat) * \
        np.cos(stations['LAT_RAD'])*hav(stations['LON_RAD']-entity_lon)

    return 2 * r * np.arcsin(np.sqrt(h1 + h2))


def closest_station(entity, stations):
    """
        Returns the tmax of the station closest to the entity
    """
    # get a single station for consistency
    first_year = entity['D.YEAR_FROM']
    last_year = entity['D.YEAR_TO']
    stations = stations[(stations['First Year'] <= first_year)
                        & (stations['Last Year'] >= last_year)]
    if len(stations) <= 0:
        # no suitable stations found
        return None
    distances = distance(entity, stations)

    return_row = stations.loc[distances.idxmin()][:]
    return_row['DISTANCE'] = distances.min()
    return_row['HYDRO_ID'] = entity['STATION_NUMBER']
    return return_row


# see http://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/HYDAT_Definition_EN.pdf for HYDAT schema

# grab hydro station data 

station_data = pd.read_csv("./index_data/filtered_station_inventory.csv",sep=',')

# get the hydro station data we care about
# table issmall enough that we don't need to build into the sql query
lakes = pd.read_csv('./index_data/filter_station_inventory.csv')
lakes_with_latlon = station_data[station_data['STATION_NUMBER'].isin(lakes['STATION_ID'])]

# get weather station data
stations = pd.read_csv('weather_station_inventory_bc.csv')
stations['LAT_RAD'] = np.radians(stations['Latitude (Decimal Degrees)'])
stations['LON_RAD'] = np.radians(stations['Longitude (Decimal Degrees)'])
stations = stations[['Latitude (Decimal Degrees)', 'Longitude (Decimal Degrees)', 'LAT_RAD',
                     'LON_RAD', 'Name', 'Climate ID', 'Station ID', 'First Year', 'Last Year', 'Elevation (m)']]

# link up the best (closest station w/data) weather to hydro station
best_weather_stations = lakes_with_latlon.apply(
    closest_station, axis=1, stations=stations).set_index('HYDRO_ID')
weather_data = pd.merge(best_weather_stations, lakes_with_latlon.set_index(
    'STATION_NUMBER'), left_index=True, right_index=True)

weather_data.to_csv('reservoir_weather_data.csv')
