import sys
import subprocess
import pandas as pd
import os
import re

dir_name = os.path.dirname(os.path.abspath(__file__))
os.chdir(dir_name)
# /ReservoirLevels/data/bathymetric_archive/bathymetric_maps/BATH_SURVEY_MAP_SHEETS_SVW/BS_MS_SVW.csv


def clean_name(inventory_list):
    # Adapted From: https://stackoverflow.com/questions/5843518/remove-all-special-characters-punctuation-and-spaces-from-string
    return re.sub('[^A-Za-z0-9]+', '', inventory_list["Name"])


def get_input_directory(weather_station):
    get_input_dir_cmd = "find ./data/weather_data/{} -type d -name '{}'"\
                            .format(weather_station["temp_name"],weather_station["Station ID"])
    return subprocess.getoutput(get_input_dir_cmd).split('\n')[0]


def cpy_over_data(weather_station):
    cpy_cmd = "cp -rn {} ./data/filtered_weather_data/{}/".format(weather_station['weather_dir'],weather_station['temp_name'])
    try:
        subprocess.run(cpy_cmd, shell=True, check=True)
    except:
        pass


weather_data = pd.read_csv('./index_data/filtered_weather_inventory.csv')
weather_data["temp_name"] = weather_data.apply(clean_name, axis=1)

weather_data['weather_dir'] = weather_data.apply(get_input_directory,axis=1)

# weather_data = weather_data.drop_duplicates('weather_dir')

weather_data.apply(cpy_over_data,axis=1)



