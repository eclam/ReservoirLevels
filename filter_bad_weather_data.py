import sys
import subprocess
import pandas as pd
import os
import re
import sqlite3
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('reddit averages').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert spark.version >= '2.3'  # make sure we have Spark 2.3+


daily_weather_schema = types.StructType([
    types.StructField('Date/Time', types.StringType()),
    types.StructField("Year", types.IntegerType()),
    types.StructField("Month", types.IntegerType()),
    types.StructField("Day", types.IntegerType()),
    types.StructField("Data Quality", types.StringType()),
    types.StructField("Max Temp (°C)", types.StringType()),
    types.StructField("Max Temp Flag", types.StringType()),
    types.StructField("Min Temp (°C)", types.StringType()),
    types.StructField("Min Temp Flag", types.StringType()),
    types.StructField("Mean Temp (°C)", types.StringType()),
    types.StructField("Mean Temp Flag", types.StringType()),
    types.StructField("Heat Deg Days (°C)", types.StringType()),
    types.StructField("Heat Deg Days Flag", types.StringType()),
    types.StructField("Cool Deg Days (°C)", types.StringType()),
    types.StructField("Cool Deg Days Flag", types.StringType()),
    types.StructField("Total Rain (mm)", types.StringType()),
    types.StructField("Total Rain Flag", types.StringType()),
    types.StructField("Total Snow (cm)", types.StringType()),
    types.StructField("Total Snow Flag", types.StringType()),
    types.StructField("Total Precip (mm)", types.StringType()),
    types.StructField("Total Precip Flag", types.StringType()),
    types.StructField("Snow on Grnd (cm)", types.StringType()),
    types.StructField("Snow on Grnd Flag", types.StringType()),
    types.StructField("Dir of Max Gust (10s deg)", types.StringType()),
    types.StructField("Dir of Max Gust Flag", types.StringType()),
    types.StructField("Spd of Max Gust (km/h)", types.StringType()),
    types.StructField("Spd of Max Gust Flag", types.StringType())
])

monthly_weather_schema = types.StructType([
    types.StructField('Date/Time', types.StringType()),
    types.StructField("Year", types.IntegerType()),
    types.StructField("Month", types.IntegerType()),
    types.StructField("Mean Max Temp (°C)", types.StringType()),
    types.StructField("Mean Max Temp Flag", types.StringType()),
    types.StructField("Mean Min Temp (°C)", types.StringType()),
    types.StructField("Mean Min Temp Flag", types.StringType()),
    types.StructField("Mean Temp (°C)", types.StringType()),
    types.StructField("Mean Temp Flag", types.StringType()),
    types.StructField("Extr Max Temp (°C)", types.StringType()),
    types.StructField("Extr Max Temp Flag", types.StringType()),
    types.StructField("Extr Min Temp (°C)", types.StringType()),
    types.StructField("Extr Min Temp Flag", types.StringType()),
    types.StructField("Total Rain (mm)", types.StringType()),
    types.StructField("Total Rain Flag", types.StringType()),
    types.StructField("Total Snow (cm)", types.StringType()),
    types.StructField("Total Snow Flag", types.StringType()),
    types.StructField("Total Precip (mm)", types.StringType()),
    types.StructField("Total Precip Flag", types.StringType()),
    types.StructField("Snow Grnd Last Day (cm)", types.StringType()),
    types.StructField("Snow Grnd Last Day Flag", types.StringType()),
    types.StructField("Dir of Max Gust (10's deg)", types.StringType()),
    types.StructField("Dir of Max Gust Flag", types.StringType()),
    types.StructField("Spd of Max Gust (km/h)", types.StringType()),
    types.StructField("Spd of Max Gust Flag", types.StringType()),
])

dir_name = os.path.dirname(os.path.abspath(__file__))
os.chdir(dir_name)
dl_path = dir_name + "/data/filtered_weather_data/"

def clean_name(archive_list):
    # Adapted From: https://stackoverflow.com/questions/5843518/remove-all-special-characters-punctuation-and-spaces-from-string
    return re.sub('[^A-Za-z0-9]+', '', archive_list["Name"])


def get_dir(inventory_list):
    return "{}{}/{}".format(dl_path, inventory_list["Name"], inventory_list["Station ID"])

def filter_temp_daily(weather_list):
    if os.path.isdir("{}/daily".format(weather_list["weather_dir"])):
        weather = spark.read.csv("{}/daily".format(weather_list["weather_dir"]), daily_weather_schema)
        filtered_weather = weather.filter(weather['Date/Time'].like("%-%-%"))\
            .filter(weather['Year'] >= 2010)\
            .select('Date/Time','Year','Month','Day',
                    "Max Temp (°C)", "Min Temp (°C)","Mean Temp (°C)", 
                    "Total Rain (mm)", "Total Snow (cm)", "Total Precip (mm)", "Snow on Grnd (cm)")
        
        # adapted from: https://stackoverflow.com/questions/48229043/python-pyspark-count-null-empty-and-nan?rq=1
        maxtemp_null_count = filtered_weather.filter((filtered_weather["Max Temp (°C)"] == "") | 
                                                  filtered_weather["Max Temp (°C)"].isNull() | 
                                                  functions.isnan(filtered_weather["Max Temp (°C)"]))\
                                         .count()
        
        mintemp_null_count = filtered_weather.filter((filtered_weather["Min Temp (°C)"] == "") | 
                                                  filtered_weather["Min Temp (°C)"].isNull() | 
                                                  functions.isnan(filtered_weather["Min Temp (°C)"]))\
                                         .count()

        meantemp_null_count = filtered_weather.filter((filtered_weather["Mean Temp (°C)"] == "") | 
                                                  filtered_weather["Mean Temp (°C)"].isNull() | 
                                                  functions.isnan(filtered_weather["Mean Temp (°C)"]))\
                                         .count()

        null_count = max(maxtemp_null_count, mintemp_null_count, )

        if meantemp_null_count < null_count:
            null_count = meantemp_null_count

        if null_count == 0 or ((null_count / filtered_weather.count()) <= 1/2):
            return 0
            
    return 1

def filter_temp_monthly(weather_list):
    if os.path.isdir("{}/monthly".format(weather_list["weather_dir"])):
        weather = spark.read.csv("{}/monthly".format(weather_list["weather_dir"]), monthly_weather_schema)
        filtered_weather = weather.filter(weather['Date/Time'].like("%-%-%"))\
                .filter(weather['Year'] >= 2010)\
                .select('Date/Time','Year','Month', 
                        "Mean Max Temp (°C)", "Mean Min Temp (°C)", "Mean Temp (°C)",
                        "Total Rain (mm)", "Total Snow (cm)", "Total Precip (mm)", "Snow Grnd Last Day (cm)")

        maxtemp_null_count = filtered_weather.filter((filtered_weather["Mean Max Temp (°C)"] == "") | 
                                                  filtered_weather["Mean Max Temp (°C)"].isNull() | 
                                                  functions.isnan(filtered_weather["Mean Max Temp (°C)"]))\
                                         .count()
        
        mintemp_null_count = filtered_weather.filter((filtered_weather["Mean Min Temp (°C)"] == "") | 
                                                  filtered_weather["Mean Min Temp (°C)"].isNull() | 
                                                  functions.isnan(filtered_weather["Mean Min Temp (°C)"]))\
                                         .count()

        meantemp_null_count = filtered_weather.filter((filtered_weather["Mean Temp (°C)"] == "") | 
                                                  filtered_weather["Mean Temp (°C)"].isNull() | 
                                                  functions.isnan(filtered_weather["Mean Temp (°C)"]))\
                                         .count()
        
        null_count = max(maxtemp_null_count, mintemp_null_count)
        
        if meantemp_null_count < null_count:
            null_count = meantemp_null_count
        
        if null_count == 0 or ((null_count / filtered_weather.count()) <= 1/2):
            return 0

    return 1

def filter_rain_daily(weather_list):
    if os.path.isdir("{}/daily".format(weather_list["weather_dir"])):
        weather = spark.read.csv("{}/daily".format(weather_list["weather_dir"]), daily_weather_schema)
        filtered_weather = weather.filter(weather['Date/Time'].like("%-%-%"))\
            .filter(weather['Year'] >= 2010)\
            .select('Date/Time','Year','Month','Day', "Total Rain (mm)", "Total Precip (mm)")
        
        # adapted from: https://stackoverflow.com/questions/48229043/python-pyspark-count-null-empty-and-nan?rq=1
        precip_null_count = filtered_weather.filter((filtered_weather["Total Precip (mm)"] == "") | 
                                                  filtered_weather["Total Precip (mm)"].isNull() | 
                                                  functions.isnan(filtered_weather["Total Precip (mm)"]))\
                                         .count()
        
        rain_null_count = filtered_weather.filter((filtered_weather["Total Rain (mm)"] == "") | 
                                                  filtered_weather["Total Rain (mm)"].isNull() | 
                                                  functions.isnan(filtered_weather["Total Rain (mm)"]))\
                                         .count()
        
        if rain_null_count <= precip_null_count:
            null_count = rain_null_count
        else:
            null_count = precip_null_count

        if null_count == 0 or ((null_count / filtered_weather.count()) <= 1/2):
            return 0

    return 1

def filter_rain_monthly(weather_list):
    if os.path.isdir("{}/monthly".format(weather_list["weather_dir"])):
        weather = spark.read.csv("{}/monthly".format(weather_list["weather_dir"]), monthly_weather_schema)
        filtered_weather = weather.filter(weather['Date/Time'].like("%-%-%"))\
            .filter(weather['Year'] >= 2010)\
            .select('Date/Time','Year','Month', "Total Rain (mm)", "Total Precip (mm)")
        
        # adapted from: https://stackoverflow.com/questions/48229043/python-pyspark-count-null-empty-and-nan?rq=1
        precip_null_count = filtered_weather.filter((filtered_weather["Total Precip (mm)"] == "") | 
                                                  filtered_weather["Total Precip (mm)"].isNull() | 
                                                  functions.isnan(filtered_weather["Total Precip (mm)"]))\
                                            .count()
        
        rain_null_count = filtered_weather.filter((filtered_weather["Total Rain (mm)"] == "") | 
                                                  filtered_weather["Total Rain (mm)"].isNull() | 
                                                  functions.isnan(filtered_weather["Total Rain (mm)"]))\
                                            .count()
          
        if rain_null_count <= precip_null_count:
            null_count = rain_null_count
        else:
            null_count = precip_null_count

        if null_count == 0 or ((null_count / filtered_weather.count()) <= 1/2):
            return 0

    return 1

def filter_snow_daily(weather_list):
    if os.path.isdir("{}/daily".format(weather_list["weather_dir"])):
        weather = spark.read.csv("{}/daily".format(weather_list["weather_dir"]), daily_weather_schema)
        filtered_weather = weather.filter(weather['Date/Time'].like("%-%-%"))\
            .filter(weather['Year'] >= 2010)\
            .select('Date/Time','Year','Month','Day', "Total Snow (cm)", "Snow on Grnd (cm)")
        
        # adapted from: https://stackoverflow.com/questions/48229043/python-pyspark-count-null-empty-and-nan?rq=1
        snow_null_count = filtered_weather.filter((filtered_weather["Total Snow (cm)"] == "") | 
                                                  filtered_weather["Total Snow (cm)"].isNull() | 
                                                  functions.isnan(filtered_weather["Total Snow (cm)"]))\
                                         .count()
        
        groundsnow_null_count = filtered_weather.filter((filtered_weather["Snow on Grnd (cm)"] == "") | 
                                                  filtered_weather["Snow on Grnd (cm)"].isNull() | 
                                                  functions.isnan(filtered_weather["Snow on Grnd (cm)"]))\
                                         .count()
        
        if snow_null_count <= groundsnow_null_count:
            null_count = snow_null_count
        else:
            null_count = groundsnow_null_count

        if null_count == 0 or ((null_count / filtered_weather.count()) <= 1/2):
            return 0

    return 1

def filter_snow_monthly(weather_list):
    if os.path.isdir("{}/monthly".format(weather_list["weather_dir"])):
        weather = spark.read.csv("{}/monthly".format(weather_list["weather_dir"]), monthly_weather_schema)
        filtered_weather = weather.filter(weather['Date/Time'].like("%-%-%"))\
            .filter(weather['Year'] >= 2010)\
            .select('Date/Time','Year','Month', "Total Snow (cm)", "Snow Grnd Last Day (cm)")
        
        # adapted from: https://stackoverflow.com/questions/48229043/python-pyspark-count-null-empty-and-nan?rq=1
        snow_null_count = filtered_weather.filter((filtered_weather["Total Snow (cm)"] == "") | 
                                                  filtered_weather["Total Snow (cm)"].isNull() | 
                                                  functions.isnan(filtered_weather["Total Snow (cm)"]))\
                                         .count()
        
        groundsnow_null_count = filtered_weather.filter((filtered_weather["Snow Grnd Last Day (cm)"] == "") | 
                                                  filtered_weather["Snow Grnd Last Day (cm)"].isNull() | 
                                                  functions.isnan(filtered_weather["Snow Grnd Last Day (cm)"]))\
                                         .count()
        
        if snow_null_count <= groundsnow_null_count:
            null_count = snow_null_count
        else:
            null_count = groundsnow_null_count

        if null_count == 0 or ((null_count / filtered_weather.count()) <= 1/2):
            return 0

    return 1

weather_fpath = "./index_data/filtered_weather_inventory.csv"
weather_inventory = pd.read_csv(weather_fpath, sep=",")
name = weather_inventory["Name"]
weather_inventory["Name"] = weather_inventory.apply(clean_name, axis=1)
weather_inventory["weather_dir"] = weather_inventory.apply(get_dir, axis=1)
weather_inventory["daily_temp_nullcount"] = weather_inventory.apply(filter_temp_daily,axis=1)
weather_inventory["monthly_temp_nullcount"] = weather_inventory.apply(filter_temp_monthly,axis=1)

weather_inventory["daily_precipiation_nullcount"] = weather_inventory.apply(filter_rain_daily,axis=1)
weather_inventory["monthly_precipiation_nullcount"] = weather_inventory.apply(filter_rain_monthly,axis=1)

weather_inventory["daily_snow_nullcount"] = weather_inventory.apply(filter_snow_daily,axis=1)
weather_inventory["monthly_snow_nullcount"] = weather_inventory.apply(filter_snow_monthly,axis=1)

weather_inventory["Name"] = name
weather_inventory = weather_inventory.drop(["weather_dir"],axis=1)

df = pd.DataFrame()
df['counts'] = ["\tfor daily temp: {}".format(weather_inventory['daily_temp_nullcount'].sum()), 
                "\tfor daily precipitation: {}".format(weather_inventory['daily_precipiation_nullcount'].sum()),
                "\tfor daily snow: {}".format(weather_inventory['daily_snow_nullcount'].sum()),
                "\tmonthly temp: {}".format(weather_inventory['monthly_temp_nullcount'].sum()),
                "\tmonthly precipiation: {}".format(weather_inventory['monthly_precipiation_nullcount'].sum()),
                "\tmonthly snow: {}".format(weather_inventory['monthly_snow_nullcount'].sum()) 
                ]

df.to_csv("./weather_data_test/nullcounts.txt",index=None, sep='\n',mode='a')

weather_inventory.to_csv("./index_data/filteredNULL_weather_inventory.csv")
