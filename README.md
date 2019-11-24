# PROJECT GUIDE ON HOW TO RUN OUR CODE: 
**WARNING**: YOU MUST DO THE FOLLOWING BEFORE PROCEEDING :
1. Make a folder called data within the same folder as the scripts -> run `mkdir data` in linux bash.

2. Files to download: 
    - [Hydat.sqlite3](https://www.canada.ca/en/environment-climate-change/services/water-overview/quantity/monitoring/survey/data-products-services/national-archive-hydat.html)

3. Run one of the following script to download weather_data:
    - A.)  run `grab_weather_data.py` if you want the COMPLETE 4GB+ data set. 
        - (Note: This is not optimized and will take 5 hours +)
    - B.) run `dl_filtered_weather_data.py` for PARTIALLY FILTERED weather data. (**RECOMMENDED**)
        - (Note: Takes less time compared to option A, but still takes a while)
    - Source: https://drive.google.com/drive/folders/1WJCDEU34c60IfOnG4rv5EPZ4IhhW9vZH

4. The program requires python3, jupyter-notebook and pyspark to be installed. 

5. The following python libraries are required to be installed: 
    - h5py, tables, joblib, seaborn, pandas, numpy, sklearn, scikitlearn. 
        - Installation command example: `pip3 install --user h5py tables seaborn`

# Steps to filter data : 
1. run `python3 hydro_and_weather_stn_filter.py` to filter out weather_station and hydro_station data 
    - hydro_station data is broken up to River, Creek, Lake -> produces these files: 
        - `filtered_creek_inventory.csv`    
        - `filtered_lake_inventory.csv`
        - `filtered_river_inventory.csv`
        - `filtered_station_inventory.csv` (combination of all of these files)

2. run `spark-submit filter_bad_weather_data.py` to filter out bad weather stations 
    - gives you a number of bad weather stations based on the data column bundle 
        - i.e. Rain, Snow, Temperature
    - produces this file:  `filteredNULL_weather_inventory.csv`
    - Some stations specialize in certain metric recording. 
        -i.e. Kelp Reefs is strictly a wind station and is terrible for our purposes

3. run `python3 closest_weather_stations.py` 
    - matches closes weather stations to the closest bodies of water 

 4. run `spark-submit weather_csv_consolidate.py` 
    - parse all the files together properly (needs to be edited and adjust properly)
    - fills in missing dates 

5.  run `combine_data.py` into a combined file format for our models 

#Analysis stuff: 
**WARNING**: DO NOT RUN ALL ON NOTEBOOKS! We have some cells for testing but intended to be ran for experimental / model optimization purposes.  
Our jupyter-notebooks has our data science analysis stuff. 
3 Different notebooks: 
- `monthly_models.ipynb` -> Contains rudimentary models 
- `daily_models.ipynb` -> contains models to generate regressor models for individual hydro stations 
    - Run all the cells up to the one that general the models and nothing else. 
    - 30 different hydro stations -> only made to run the first five 
    - if you want the other models, run the function `create_models(key) ` to generate that model 
    - if you want to see all keys, the daily keys variable will contain that 
-  `generate_regressor_input` -> allows us to use the regressors by only providing a date range 



