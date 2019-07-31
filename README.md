# Reservoir Levels

reservoir\_list.csv - manually generated list of reservoirs and hydro stations

reservoir\_weather\_data.csv - list of hydro station weather station pairings, including some metadata

weather\_data\_test/\*.csv - by station\_id, daily weather data in a single file

`combined_data.hdf` - dataframe of combined level + weather data. Each dataframe stored under key `hydro_<hydro_station_id>`


steps to run:
- run the get-weather-data script
- run `closest_weather_stations.py` to get the best weather station for each hydro station
- run `weather_csv_consolidate.py` to collect all the daily data for each weather station and combine it into one file (spark)
- run `combine_data.py` to consolidate the level and weather data (TODO: better name)


(eric) steps to run to filter data: 
- run `hydro_and_weather_stn_filter.py` to filter out weather_station and hydro_station data 
    - hydro_station data is broken up to River, Creek, Lake 
        - there is a csv that is combination of all of these files 
- run `cp_move_data_filtered` to move the data to a filtered folder (NOT MANDATORY)
- run `filter_bad_weather_data.py` to filter out bad weather stations 
    - gives you a number of bad weather stations based on the data column bundle 
        - i.e. Rain, Snow, Temperature
- run `weather_csv_consolidate.py` to parse all the files together properly (needs to be edited and adjust properly)
- run `combine_data.py` into workable file format for our models (need to be eventually done)
