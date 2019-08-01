https://gis.stackexchange.com/questions/241023/calculating-a-volume-from-points
https://medium.com/analytics-vidhya/satellite-imagery-analysis-with-python-3f8ccf8a7c32

Things to do: 
- (Maybe) Get GHCN data to compensate Canadian Govt's horrible data set 
    - https://www.ncdc.noaa.gov/data-access/land-based-station-data/land-based-datasets/global-historical-climatology-network-ghcn


- figure out a formula on how to approximate volume from Surface Area + elevation 


- BC hydro station data 5 year range 2000-2018 -> need to do that on hydat file 
- match that w/ weather station data -> 2000-2018 -> do that in closest weather station file


y_train = hydro station levels
x_train = weather data 


Things to do : 
1. finished consolidate data & combine data to get the hdf file 
2. Cluster data 
    - group data by months  
    - 3 cluster your data center -> return the values of the centers 
        - cluster rain, temperature 
        clusters of years of those months 
    - stdev 
    (looking for a base where we can build data off of -> get stdev )
(look into other forms of clustering)