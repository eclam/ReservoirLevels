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
