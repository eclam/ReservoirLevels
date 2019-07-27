# Figuring out big of a polygon we need for nasa dataset
import sys
import subprocess
import pandas as pd
import os
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap


dir_name = os.path.dirname(os.path.abspath(__file__))
os.chdir(dir_name)
# print(dir_name)

fpath = dir_name + "/test_data/weather_station_inventory_bc.csv"
inventory_list = pd.read_csv(fpath, sep=",")
inventory_list = inventory_list[[
    "Name", "Station ID", "Latitude (Decimal Degrees)", "Longitude (Decimal Degrees)"]]


# Adapted From: https://stackoverflow.com/questions/44488167/plotting-lat-long-points-using-basemap

list_lat = inventory_list['Latitude (Decimal Degrees)'].to_list()
list_lon = inventory_list['Longitude (Decimal Degrees)'].to_list()

margin = 2
lat_min = min(list_lat) - margin
lat_max = max(list_lat) + margin
lon_min = min(list_lon) - margin
lon_max = max(list_lon) + margin


m = Basemap(llcrnrlon=lon_min,
            llcrnrlat=lat_min,
            urcrnrlon=lon_max,
            urcrnrlat=lat_max,
            lat_0=(lat_max - lat_min)/2,
            lon_0=(lon_max - lon_min)/2,
            projection='merc',
            resolution='h',
            area_thresh=10000.,)

m.drawcoastlines()
m.drawcountries()
m.drawstates()

x, y = m(list_lon, list_lat)  # transform coordinates

m.scatter(x, y, 10, marker='o', color='Red')
plt.show()
