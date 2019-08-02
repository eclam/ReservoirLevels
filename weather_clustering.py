import pandas as pd
import sys

import numpy as np
from sklearn.cluster import KMeans
import seaborn

seaborn.set()

def cluster_day(filtered_df):
    if len(filtered_df) == 0:
        return None
    print(len(filtered_df))
    return KMeans(n_clusters=3).fit(filtered_df).cluster_centers_



# obviously, this is terrible, but our dataframes are fairly small
# (timeit gave me <10 seconds for 25 years of weather)
# optimize later?
# this gives the (centers of the) actual clusters
for month in range(1,12):
    for day in range(1,30):
        # filter on the month/day combo
        date_str = '-{month:02d}-{day:02d}'.format(month=month, day=day)
        print(month, day, cluster_day(df.filter(regex=r'{}'.format(date_str), axis=0)))
        
        
# this gives stddevs
df.groupby(['Day', 'Month']).agg(np.std)

#save to dataframe weather_{weather_station_id}.hdf
# save the stddev with key='std'
# save the centers with key=???
