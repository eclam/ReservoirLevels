import pandas as pd
import subprocess, os, signal, pathlib, errno, re

dir_name = os.path.dirname(os.path.abspath(__file__))
os.chdir(dir_name)

print("***WARNING***: YOU ONLY NEED TO RUN THIS ONCE!!!!")

"""
 HEADER NAMES =["Name","Province","Climate ID","Station ID","WMO ID","TC ID","Latitude (Decimal Degrees)",
                "Longitude (Decimal Degrees)","Latitude","Longitude","Elevation (m)","First Year","Last Year","HLY First Year",
                "HLY Last Year","DLY First Year","DLY Last Year","MLY First Year","MLY Last Year"]
"""
inventory_file = dir_name + "/test_data/weather_station_inventory_bc.csv"
inventory_list = pd.read_csv(inventory_file, sep=",")
inventory_list = inventory_list[["Name","Station ID", "Latitude", "Longitude",            
                                    "First Year","Last Year",
                                    "HLY First Year","HLY Last Year",
                                    "DLY First Year","DLY Last Year",
                                    "MLY First Year","MLY Last Year"]]

dl_path = dir_name + "/data/weather_data/"

def clean_name(inventory_list):
    # Adapted From: https://stackoverflow.com/questions/5843518/remove-all-special-characters-punctuation-and-spaces-from-string
    return re.sub('[^A-Za-z0-9]+', '', inventory_list["Name"])
   

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
        print("{} does not have monthly data".format(inventory_list["Name"]))
        return

    mk_dir_path = dl_path + "{}/{}/monthly".format(inventory_list["Name"],inventory_list["Station ID"])
    try:
        os.makedirs(mk_dir_path) #folder for station ID -> if names are not unique
    except OSError as e:
        print("{} Monthly Folder Exists".format(inventory_list["Name"]))
        pass
    
    curl_cmd = "(cd {}; ".format(mk_dir_path) + \
                "curl -JLO \"http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv" + \
                "&stationID={}".format(inventory_list["Station ID"]) + \
                "&Year={}".format(inventory_list["MLY First Year"]) + \
                "&Month={}".format("1") + \
                "&timeframe={}&submit=Download+Data\";wait)".format("3")
    
    subprocess.run(curl_cmd , shell=True, check=True)    
    

def dl_daily_data(inventory_list):
    if inventory_list["DLY First Year"] == 0 or inventory_list["DLY Last Year"] == 0:
        print("{} does not have daily data".format(inventory_list["Name"]))
        return

    mk_dir_path = dl_path + "{}/{}/daily".format(inventory_list["Name"],inventory_list["Station ID"])
    try:
        os.makedirs(mk_dir_path) #folder for station ID -> if names are not unique
    except OSError as e:
        print("{} Daily Folder Exists".format(inventory_list["Name"]))
        pass
    
    curl_cmd = "(cd {}; ".format(mk_dir_path) + \
                "for year in `seq {} {}`;do curl -JLO ".format(inventory_list["DLY First Year"], inventory_list["DLY Last Year"]) + \
                "\"http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv" + \
                "&stationID={}".format(inventory_list["Station ID"]) + \
                "&Year=${" + "year}" + \
                "&Month={}".format("1") + \
                "&timeframe={}&submit=Download+Data\";wait;done)".format("2")
    
    subprocess.run(curl_cmd , shell=True, check=True) 

def dl_hourly_data(inventory_list):
    if inventory_list["HLY First Year"] == 0 or inventory_list["HLY Last Year"] == 0:
        print("{} does not have daily data".format(inventory_list["Name"]))
        return

    mk_dir_path = dl_path + "{}/{}/daily".format(inventory_list["Name"],inventory_list["Station ID"])
    try:
        os.makedirs(mk_dir_path) #folder for station ID -> if names are not unique
    except OSError as e:
        print("{} Daily Folder Exists".format(inventory_list["Name"]))
        pass

    curl_cmd = "(cd {}; ".format(mk_dir_path) + \
                "for year in `seq {} {}`;do ".format(inventory_list["DLY First Year"], inventory_list["DLY Last Year"]) + \
                "for month in `seq 1 12`;do " + \
                "curl -JLO " + \
                "\"http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv" + \
                "&stationID={}".format(inventory_list["Station ID"]) + \
                "&Year=${" + "year}" + \
                "&Month=${" +"month}" + \
                "&timeframe={}&submit=Download+Data\";wait;done;done)".format("1")
    
    subprocess.run(curl_cmd , shell=True, check=True)


# Adapted from: https://stackoverflow.com/questions/11350770/select-by-partial-string-from-a-pandas-dataframe
# print(inventory_list.loc[inventory_list['Name'].str.contains("VANCOUVER I")]) #selects rows that contains this value in specified column


# Clean Data
inventory_list["Name"] = inventory_list.apply(clean_name, axis=1)
inventory_list = inventory_list.fillna(0)
inventory_list[["First Year","Last Year","HLY First Year","HLY Last Year",
                "DLY First Year","DLY Last Year",
                "MLY First Year","MLY Last Year"]] = inventory_list[["First Year","Last Year","HLY First Year",
                                                                    "HLY Last Year","DLY First Year","DLY Last Year",
                                                                    "MLY First Year","MLY Last Year"]].apply(pd.to_numeric, downcast='integer') 


inventory_list.apply(make_station_folders, axis=1)
inventory_list.apply(dl_monthly_data, axis=1)
inventory_list.apply(dl_daily_data, axis=1)
inventory_list.apply(dl_hourly_data, axis=1)

