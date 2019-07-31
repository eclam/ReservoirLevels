import sys
import subprocess
import pandas as pd
import os
import re
import sqlite3

dir_name = os.path.dirname(os.path.abspath(__file__))
os.chdir(dir_name)

def clean_name(archive_list):
    # Adapted From: https://stackoverflow.com/questions/5843518/remove-all-special-characters-punctuation-and-spaces-from-string
    return re.sub('[^A-Za-z0-9]+', '', archive_list["Name"])

def river_vs_lake_cleaner(inventory):
      return re.sub('\s(AT|NEAR|BELOW|ABOVE) .*','', inventory['STATION_NAME'])


"""
 HEADER NAMES =["Name","Province","Climate ID","Station ID","WMO ID","TC ID","Latitude (Decimal Degrees)",
                "Longitude (Decimal Degrees)","Latitude","Longitude","Elevation (m)","First Year","Last Year","HLY First Year",
                "HLY Last Year","DLY First Year","DLY Last Year","MLY First Year","MLY Last Year"]
"""
fpath = dir_name + "/index_data/weather_station_inventory_bc.csv"
weather_inventory = pd.read_csv(fpath, sep=",")
weather_inventory = weather_inventory[["Name","Station ID", "Latitude", "Longitude",            
                                    "First Year","Last Year",
                                    "HLY First Year","HLY Last Year",
                                    "DLY First Year","DLY Last Year",
                                    "MLY First Year","MLY Last Year"]]

# weather_inventory["Name"] = weather_inventory.apply(clean_name, axis=1)

weather_inventory = weather_inventory.fillna(0)
weather_inventory[["First Year","Last Year","HLY First Year","HLY Last Year",
                "DLY First Year","DLY Last Year",
                "MLY First Year","MLY Last Year"]] = weather_inventory[["First Year","Last Year","HLY First Year",
                                                                    "HLY Last Year","DLY First Year","DLY Last Year",
                                                                    "MLY First Year","MLY Last Year"]].apply(pd.to_numeric, downcast='integer') 

weather_inventory = weather_inventory[(weather_inventory['Last Year']>= 2015) & 
                                      (weather_inventory['First Year']<=2010)].reset_index(drop=True)

weather_inventory.to_csv("./index_data/filtered_weather_inventory.csv")

########################################################################################################
db_filename = './data/Hydat.sqlite3'
db_conn = sqlite3.connect(db_filename)

good_stations_query = """SELECT F.*
                         FROM (SELECT S.*, D.YEAR_FROM, D.YEAR_TO, D.RECORD_LENGTH 
                               FROM (SELECT STATION_NUMBER, STATION_NAME 
                                     FROM STATIONS 
                                     WHERE STATIONS.PROV_TERR_STATE_LOC='BC' AND 
                                           HYD_STATUS =='A' AND REAL_TIME == 1
                                     ) S
                                    INNER JOIN
                                    (SELECT STATION_NUMBER, YEAR_FROM, YEAR_TO, RECORD_LENGTH 
                                     FROM STN_DATA_RANGE 
                                     WHERE YEAR_FROM <= 2010 AND 
                                     YEAR_TO == 2018 AND 
                                     RECORD_LENGTH>=5 
                                    ) D
                                    ON S.STATION_NUMBER = D.STATION_NUMBER) F
                        ORDER BY F.STATION_NAME ASC;"""

station_inventory = pd.read_sql_query(good_stations_query, db_conn)
station_inventory['temp_name'] = station_inventory.apply(river_vs_lake_cleaner,axis=1)

river_inventory = station_inventory[station_inventory['temp_name'].str.contains("RIVER")]
creek_inventory = station_inventory[(station_inventory['temp_name'].str.contains("CREEK"))]
lake_inventory = station_inventory[station_inventory['temp_name'].str.contains("LAKE")]

river_inventory.to_csv("./index_data/filtered_river_inventory.csv")
creek_inventory.to_csv("./index_data/filtered_creek_inventory.csv")
lake_inventory.to_csv("./index_data/filtered_lake_inventory.csv")
station_inventory.to_csv("./index_data/filtered_station_inventory.csv")
