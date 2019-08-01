import sys
import subprocess
import pandas as pd
import os
import re
import sqlite3
from pyspark.sql import SparkSession, functions, types

dir_name = os.path.dirname(os.path.abspath(__file__))
os.chdir(dir_name)

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

dl_path = dir_name + "/data/filtered_weather_data/"

def clean_name(archive_list):
    # Adapted From: https://stackoverflow.com/questions/5843518/remove-all-special-characters-punctuation-and-spaces-from-string
    return re.sub('[^A-Za-z0-9]+', '', archive_list["Name"])

def get_dir(inventory_list):
    return "{}{}/{}".format(dl_path, inventory_list["temp_name"], inventory_list["Station ID"])

def get_date_value(weather_list):
    # Adapted From: https://stackoverflow.com/questions/5843518/remove-all-special-characters-punctuation-and-spaces-from-string
    return re.sub('[^A-Za-z0-9]+', '', weather_list["Date/Time"])

def transform_monthly_into_daily_weather(weather_list): #NOT DONE
    if weather_list["MLY First Year"] == 0 or weather_list["MLY First year"] == 0: #only need the first year
        return

    if os.path.isdir("{}/monthly".format(weather_list["weather_dir"])):
        weather = spark.read.csv("{}/monthly".format(weather_list["weather_dir"]), monthly_weather_schema)
        filtered_monthly = weather.filter(weather['Date/Time'].like("%-%-%"))\
                                  .filter(weather['Year'] >= 2010)\
                                  .select('Date/Time','Year','Month', 
                                          "Mean Max Temp (°C)", "Mean Min Temp (°C)", "Mean Temp (°C)",
                                          "Total Rain (mm)", "Total Snow (cm)", 
                                          "Total Precip (mm)", "Snow Grnd Last Day (cm)")
        
        filtered_monthly = filtered_monthly.toPandas()
        filtered_monthly['date_value'] = filtered_monthly.apply(get_date_value,axis=1)
        filtered_monthly = filtered_monthly[(filtered_monthly['date_value'].astype(int) < 201907)]
        filtered_monthly = filtered_monthly[(filtered_monthly['date_value'].astype(int) > 199012)]

        # filtered_monthly = filtered_monthly.apply(consolidate_daily_for_monthly,axis=1)

        filtered_monthly = filtered_monthly.drop(['date_value'],axis=1)
        filtered_monthly = filtered_monthly.set_index('Date/Time')\
                                           .to_csv('./data/consolidated_weather_data/daily/{}.csv')\
                                           .format(weather_list["Station ID"]))

def consolidate_daily_weather(weather_list):
    if weather_list["DLY First Year"] == 0 or weather_list["DLY Last Year"] == 0:
        return

    if os.path.isdir("{}/daily".format(weather_list["weather_dir"])):
        weather = spark.read.csv("{}/daily".format(weather_list["weather_dir"]), daily_weather_schema)
        
        filtered_weather = weather.filter(weather['Date/Time'].like("%-%-%"))\
                                .select('Date/Time','Year','Month','Day',
                                        "Max Temp (°C)", "Min Temp (°C)","Mean Temp (°C)", 
                                        "Total Rain (mm)", "Total Snow (cm)", 
                                        "Total Precip (mm)", "Snow on Grnd (cm)")
        
        filtered_weather = filtered_weather.toPandas()
        filtered_weather['date_value'] = filtered_weather.apply(get_date_value,axis=1)

        # Cut out data where it has not arrived yet and prior to set time
        filtered_weather = filtered_weather[(filtered_weather['date_value'].astype(int) < 20190716)]
        filtered_weather = filtered_weather[(filtered_weather['date_value'].astype(int) > 19901231)]

        # Adapted From: https://stackoverflow.com/questions/27905295/how-to-replace-nans-by-preceding-values-in-pandas-dataframe
        filtered_weather = filtered_weather.sort_values(['Year', 'Month', 'Day'], ascending=[True, True, True])\
                                           .fillna(method='ffill')
        filtered_weather = filtered_weather.sort_values(['Year', 'Month', 'Day'], ascending=[True, True, True])\
                                           .groupby(['Month'], as_index=False)\
                                           .fillna(method='ffill')\
                                           .reset_index()

        filtered_weather = filtered_weather.drop(['date_value'],axis=1)
        filtered_weather = filtered_weather.set_index('Date/Time')\
                        .to_csv('./data/consolidated_weather_data/daily/{}.csv'.format( weather_list["Station ID"] ))
        # .sort_values(['Year', 'Month', 'Day'], ascending=[True, True, True])\

# def consolidate_daily_for_monthly(df):
    # if df['Mean Max Temp (°C)'].isnull():
    # if df['Mean Min Temp (°C)'].isnull():
    # if df["Mean Temp (°C)"].isnull():
    # if df["Extr Max Temp (°C)"].isnull():
    # if df["Extr Min Temp (°C)"].isnull():
    # if df["Total Rain (mm)"].isnull():
    # if df["Total Snow (cm)"].isnull():
    # if df["Total Precip (mm)"].isnull():
    # if df["Snow Grnd Last Day (cm)"].isnull():





weather_fpath = "./index_data/filteredNULL_weather_inventory.csv"
weather_inventory = pd.read_csv(weather_fpath, sep=",")

weather_inventory["temp_name"] = weather_inventory.apply(clean_name, axis=1)
weather_inventory["weather_dir"] = weather_inventory.apply(get_dir, axis=1)

weather_inventory.apply(transform_monthly_into_daily_weather,axis=1)
# weather_inventory.apply(consolidate_daily_weather,axis=1)
