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




(eric) steps to run IN FOLLOWING ORDER to filter and combine data: 
- run `python3 hydro_and_weather_stn_filter.py` to filter out weather_station and hydro_station data 
    - hydro_station data is broken up to River, Creek, Lake -> produces these files: 
        - `filtered_creek_inventory.csv`    
        - `filtered_lake_inventory.csv`
        - `filtered_river_inventory.csv`
        - `filtered_station_inventory.csv` (combination of all of these files)

- run `python3 cpy_move_data.py` (NOT MANDATORY -> for Eric's use)
- run `python3 dl_filtered_weather_data.py` to move the data to a filtered folder (NOT MANDATORY -> Eric's use)
- run `spark-submit --master=local[2] filter_bad_weather_data.py` to filter out bad weather stations 
    - gives you a number of bad weather stations based on the data column bundle 
        - i.e. Rain, Snow, Temperature
    - produces this file:  `filteredNULL_weather_inventory.csv`
- run `python3 closest_weather_stations.py` 
    - <IMPORTANT NOTE>: this COULD does filter whether distances <50 km 
- run `spark-submit --master=local[2] weather_csv_consolidate.py` 
    - parse all the files together properly (needs to be edited and adjust properly)
- run `combine_data.py` into workable file format for our models (need to be eventually done)


NOTE: 
- Some stations specialize in certain metric recording. 
    -I.E. Kelp Reefs is a wind station and is terrible for our purposes