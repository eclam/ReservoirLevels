https://gis.stackexchange.com/questions/241023/calculating-a-volume-from-points
https://medium.com/analytics-vidhya/satellite-imagery-analysis-with-python-3f8ccf8a7c32

Things to do: 
- (Maybe) Get GHCN data to compensate Canadian Govt's horrible data set 
    - https://www.ncdc.noaa.gov/data-access/land-based-station-data/land-based-datasets/global-historical-climatology-network-ghcn


- figure out a formula on how to approximate volume from Surface Area + elevation 
    - found that we do have bathymetric map of some lakes -> need to find how to get volume 
    - need to download the bathymetric maps -> .tif links are all in CSV 
        - need to join current lake list w/ the bathymetric csv to determine which to DL 
    - convert .tif files into 3d models to get volume -> height should be the deciding parameter to get the volume 
        - https://community.esri.com/thread/64610
    



- BC hydro station data 5 year range 2000-2018 -> need to do that on hydat file 
- match that w/ weather station data -> 2000-2018 -> do that in closest weather station file

- DL soil moisture data from ->2015-2018 ??? (maybe...Probably not)

- figure out how to deal with weather data -> must have data +5 years 

y_train = hydro station levels
x_train = weather data 