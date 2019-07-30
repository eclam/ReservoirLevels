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
- Need to have data merged 
- If NULL, look at pervious years if it exists and fill it in to current data set -> or if GHCN has something then use that 
- Figure out whether if we want to use daily or monthly in training 
