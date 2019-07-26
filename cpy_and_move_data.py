import sys
import subprocess
import pandas as pd
import os
import re

dir_name = os.path.dirname(os.path.abspath(__file__))
os.chdir(dir_name)


def clean_name(inventory_list):
    # Adapted From: https://stackoverflow.com/questions/5843518/remove-all-special-characters-punctuation-and-spaces-from-string
    return re.sub('[^A-Za-z0-9]+', '', inventory_list["Name"])


def get_input_directory(station_name):
    get_input_dir_cmd = "find -type d -name '{}'".format(station_name)
    return subprocess.getoutput(get_input_dir_cmd).split('\n')[0]


def cpy_over_data(weather_dir):
    cpy_cmd = "cp -r {} ./test_data/weather_data/".format(weather_dir)
    subprocess.run(cpy_cmd, shell=True, check=True)


station_data = pd.read_csv('reservoir_weather_data.csv')
station_data["Name"] = station_data.apply(clean_name, axis=1)
station_data['WEATHER_DIR'] = station_data['Name'].apply(
    get_input_directory)

station_data = station_data.drop_duplicates('WEATHER_DIR')

station_data['WEATHER_DIR'].apply(cpy_over_data)


print(station_data)
