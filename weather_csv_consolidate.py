import sys
from pyspark.sql import SparkSession, functions, types

import subprocess
import pandas as pd
import os

dir_name = os.path.dirname(os.path.abspath(__file__))
os.chdir(dir_name)

spark = SparkSession.builder.appName('reddit averages').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert spark.version >= '2.3'  # make sure we have Spark 2.3+


def get_input_directory(station_id):
    get_input_dir_cmd = "find -type d -name '{}'".format(station_id)
    return subprocess.getoutput(get_input_dir_cmd).split('\n')[0]


weather_schema = types.StructType([
    types.StructField('Date/Time', types.StringType()),
    types.StructField("Year", types.StringType()),
    types.StructField("Month", types.StringType()),
    types.StructField("Day", types.StringType()),
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

station_data = pd.read_csv('reservoir_weather_data.csv')
station_data['WEATHER_DIR'] = station_data['Station ID'].apply(
    get_input_directory)
cpy_cmd = ""
subprocess.run(cpy_cmd, shell=True, check=True)


print(station_data)

for input_dir in station_data['WEATHER_DIR']:
    station_id = input_dir.split('/')[-1]

    if os.path.isdir("{}/daily".format(input_dir)):
        weather = spark.read.csv("{}/daily".format(input_dir), weather_schema)
        filtered_weather = weather.filter(weather['Date/Time'].like("%-%-%")).select(
            'Date/Time', "Total Rain (mm)", "Total Snow (cm)", "Total Precip (mm)",
            "Max Temp (°C)", "Min Temp (°C)","Mean Temp (°C)", "Snow on Grnd (cm)")
        filtered_weather.show()
        # show a bit for reference
        # toPandas should be fine, we have at most one entry per day and at most 70 years of weather data
        filtered_weather.toPandas().set_index(
            'Date/Time').to_csv('{}/weather_data_test/{}.csv'.format(dir_name, station_id))
    else:
        print('not present')
