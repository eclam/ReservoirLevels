import sys
import subprocess
import pandas as pd
import os
import re
import sqlite3

dir_name = os.path.dirname(os.path.abspath(__file__))
os.chdir(dir_name)

dl_path = dir_name + "/data/filtered_weather_data/"

def clean_name(archive_list):
    # Adapted From: https://stackoverflow.com/questions/5843518/remove-all-special-characters-punctuation-and-spaces-from-string
    return re.sub('[^A-Za-z0-9]+', '', archive_list["Name"])

# Make Station Folders 
def make_station_folders(inventory_list):
    mk_dir_path = dl_path + "{}".format(inventory_list["Name"]) 

    # Adapted From: https://stackoverflow.com/questions/273192/how-can-i-safely-create-a-nested-directory
    try:
        os.makedirs(mk_dir_path) #Folder for station 
    except OSError as e:
        pass
    
    mk_dir_path = mk_dir_path + "/{}".format(inventory_list["Station ID"])
    try:
        os.makedirs(mk_dir_path) #folder for station ID -> if names are not unique
    except OSError as e:
        pass
    
def dl_monthly_data(inventory_list):
    if inventory_list["MLY First Year"] == 0: #only need the first year
        return

    mk_dir_path = dl_path + "{}/{}/monthly".format(inventory_list["Name"],inventory_list["Station ID"])
    try:
        os.makedirs(mk_dir_path) #folder for station ID -> if names are not unique
    except OSError as e:
        pass

    if len(os.listdir(mk_dir_path) ) > 0:
        return
    
    curl_cmd = "(cd {}; ".format(mk_dir_path) + \
                "curl -sJLO \"http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv" + \
                "&stationID={}".format(inventory_list["Station ID"]) + \
                "&Year={}".format(inventory_list["MLY First Year"]) + \
                "&Month={}".format("1") + \
                "&timeframe={}&submit=Download+Data\";)".format("3")
    try:
        subprocess.run(curl_cmd , shell=True, check=True)    
    except:
        pass
    
def dl_daily_data(inventory_list):
    if inventory_list["DLY First Year"] == 0 or inventory_list["DLY Last Year"] == 0:
        return

    mk_dir_path = dl_path + "{}/{}/daily".format(inventory_list["Name"],inventory_list["Station ID"])
    try:
        os.makedirs(mk_dir_path) #folder for station ID -> if names are not unique
    except OSError as e:
        pass
        
    curl_cmd = "(cd {}; ".format(mk_dir_path) + \
                "for year in `seq {} {}`;do curl -JLO ".format(inventory_list["DLY First Year"], inventory_list["DLY Last Year"]) + \
                "\"http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv" + \
                "&stationID={}".format(inventory_list["Station ID"]) + \
                "&Year=${" + "year}" + \
                "&Month={}".format("1") + \
                "&timeframe={}&submit=Download+Data\"; done)".format("2")
    
    try:
        subprocess.run(curl_cmd , shell=True, check=True)    
    except:
        pass

def dl_hourly_data(inventory_list):
    if inventory_list["HLY First Year"] == 0 or inventory_list["HLY Last Year"] == 0:
        return 0

    elif (inventory_list["MLY First Year"] > 0 or inventory_list["MLY Last Year"] > 0) or \
            (inventory_list["DLY First Year"] > 0 or inventory_list["DLY Last Year"] > 0):
        return 0

    mk_dir_path = dl_path + "{}/{}/hourly".format(inventory_list["Name"],inventory_list["Station ID"])
    try:
        os.makedirs(mk_dir_path) #folder for station ID -> if names are not unique
    except OSError as e:
        print("{} Hourly Folder Exists".format(inventory_list["Name"]))
        pass

    curl_cmd = "(cd {}; ".format(mk_dir_path) + \
                "for year in `seq {} {}`;do ".format(inventory_list["HLY First Year"], inventory_list["HLY Last Year"]) + \
                "for month in `seq 1 12`;do " + \
                "curl -sJLO " + \
                "\"http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv" + \
                "&stationID={}".format(inventory_list["Station ID"]) + \
                "&Year=${" + "year}" + \
                "&Month=${" +"month}" + \
                "&timeframe={}&submit=Download+Data\";done ; done)".format("1")
    
    try:
        subprocess.run(curl_cmd , shell=True, check=True)    
    except:
        return -1
    return 1

def rm_hourly_data(inventory_list):
    
    if inventory_list["HLY First Year"] == 0 or inventory_list["HLY Last Year"] == 0:
        return

    elif (inventory_list["MLY First Year"] == 0 or inventory_list["MLY Last Year"] == 0) and \
            (inventory_list["DLY First Year"] == 0 or inventory_list["DLY Last Year"] == 0):
        return
    else:
        dir_path = dir_name +"/data/weather_data/" + \
                      "{}/{}/hourly/".format(inventory_list["Name"],inventory_list["Station ID"])

        curl_cmd = "rm -rf {}".format(dir_path)
        try:
            subprocess.run(curl_cmd , shell=True, check=True)    
        except:
            pass
        

def mv_filtered_data(inventory_list):
    dir_path = dir_name + "/data/weather_data/{}/".format(inventory_list["Name"])
    curl_cmd = "cp -r  {} {}".format(dir_path, dl_path)
    try:
        subprocess.run(curl_cmd , shell=True, check=True)
    except:
        pass


weather_fpath = "./index_data/filtered_weather_inventory.csv"
weather_inventory = pd.read_csv(weather_fpath, sep=",")
weather_inventory = weather_inventory.fillna(0)
name = weather_inventory["Name"]
weather_inventory["Name"] = weather_inventory.apply(clean_name, axis=1)


# weather_inventory = weather_inventory[(weather_inventory['Last Year']>= 2015) & 
#                                       (weather_inventory['First Year']<=2000)].reset_index(drop=True)

# DONE COMMANDS 
# weather_inventory.apply(rm_hourly_data, axis=1)
# weather_inventory.apply(mv_filtered_data,axis=1)
# weather_inventory.apply(make_station_folders, axis=1)



weather_inventory.apply(dl_daily_data, axis=1) #i fucked up something. gotta re-dl it 
# weather_inventory.apply(dl_monthly_data, axis=1)

# weather_inventory["hourly_only_flag"] = weather_inventory.apply(dl_hourly_data, axis=1)

# weather_inventory["Name"] = name
# weather_inventory.to_csv(dir_name+"/index_data/filtered_weather_inventory.csv")
