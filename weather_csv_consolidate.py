import sys
import subprocess
import pandas as pd
import os
import re
import sqlite3
from pyspark.sql import SparkSession, functions, types

dir_name = os.path.dirname(os.path.abspath(__file__))
os.chdir(dir_name)

spark = SparkSession.builder.appName('reddit averages').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert spark.version >= '2.3'  # make sure we have Spark 2.3+

daily_weather_schema = types.StructType([
    types.StructField('Date/Time', types.StringType()),
    types.StructField("Year", types.IntegerType()),
    types.StructField("Month", types.IntegerType()),
    types.StructField("Day", types.IntegerType()),
    types.StructField("Data Quality", types.StringType()),
    types.StructField("Max Temp (°C)", types.FloatType()),
    types.StructField("Max Temp Flag", types.StringType()),
    types.StructField("Min Temp (°C)", types.FloatType()),
    types.StructField("Min Temp Flag", types.StringType()),
    types.StructField("Mean Temp (°C)", types.FloatType()),
    types.StructField("Mean Temp Flag", types.StringType()),
    types.StructField("Heat Deg Days (°C)", types.FloatType()),
    types.StructField("Heat Deg Days Flag", types.StringType()),
    types.StructField("Cool Deg Days (°C)", types.FloatType()),
    types.StructField("Cool Deg Days Flag", types.StringType()),
    types.StructField("Total Rain (mm)", types.FloatType()),
    types.StructField("Total Rain Flag", types.StringType()),
    types.StructField("Total Snow (cm)", types.FloatType()),
    types.StructField("Total Snow Flag", types.StringType()),
    types.StructField("Total Precip (mm)", types.FloatType()),
    types.StructField("Total Precip Flag", types.StringType()),
    types.StructField("Snow on Grnd (cm)", types.FloatType()),
    types.StructField("Snow on Grnd Flag", types.StringType()),
    types.StructField("Dir of Max Gust (10s deg)", types.FloatType()),
    types.StructField("Dir of Max Gust Flag", types.StringType()),
    types.StructField("Spd of Max Gust (km/h)", types.FloatType()),
    types.StructField("Spd of Max Gust Flag", types.StringType())
])

monthly_weather_schema = types.StructType([
    types.StructField('Date/Time', types.StringType()),
    types.StructField("Year", types.IntegerType()),
    types.StructField("Month", types.IntegerType()),
    types.StructField("Mean Max Temp (°C)", types.FloatType()),
    types.StructField("Mean Max Temp Flag", types.StringType()),
    types.StructField("Mean Min Temp (°C)", types.FloatType()),
    types.StructField("Mean Min Temp Flag", types.StringType()),
    types.StructField("Mean Temp (°C)", types.FloatType()),
    types.StructField("Mean Temp Flag", types.StringType()),
    types.StructField("Extr Max Temp (°C)", types.FloatType()),
    types.StructField("Extr Max Temp Flag", types.FloatType()),
    types.StructField("Extr Min Temp (°C)", types.FloatType()),
    types.StructField("Extr Min Temp Flag", types.FloatType()),
    types.StructField("Total Rain (mm)", types.FloatType()),
    types.StructField("Total Rain Flag", types.StringType()),
    types.StructField("Total Snow (cm)", types.FloatType()),
    types.StructField("Total Snow Flag", types.StringType()),
    types.StructField("Total Precip (mm)", types.FloatType()),
    types.StructField("Total Precip Flag", types.StringType()),
    types.StructField("Snow Grnd Last Day (cm)", types.FloatType()),
    types.StructField("Snow Grnd Last Day Flag", types.StringType()),
    types.StructField("Dir of Max Gust (10's deg)", types.FloatType()),
    types.StructField("Dir of Max Gust Flag", types.StringType()),
    types.StructField("Spd of Max Gust (km/h)", types.FloatType()),
    types.StructField("Spd of Max Gust Flag", types.StringType()),
])

dl_path = dir_name + "/data/filtered_weather_data/"

def clean_name(archive_list):
    # Adapted From: https://stackoverflow.com/questions/5843518/remove-all-special-characters-punctuation-and-spaces-from-string
    return re.sub('[^A-Za-z0-9]+', '', archive_list["Name"])

def get_dir(inventory_list):
    return "{}{}/{}".format(dl_path, inventory_list["temp_name"], inventory_list["Station ID"])


def month_avg_filling_nulls(df, month_avgs):
    month = month_avgs[month_avgs['Month'] == df['Month']]
    month = month.iloc[0]

    if pd.isnull(df['Mean Max Temp (°C)']):
        df['Mean Max Temp (°C)'] = month['Mean Max Temp (°C)']
    
    if pd.isnull(df['Mean Min Temp (°C)']):
        df['Mean Min Temp (°C)'] = month['Mean Min Temp (°C)']
        
    if pd.isnull(df["Mean Temp (°C)"]):
        df['Mean Temp (°C)'] = month['Mean Temp (°C)']

    if pd.isnull(df["Extr Max Temp (°C)"]):
        df['Extr Max Temp (°C)'] = month['Extr Max Temp (°C)']

    if pd.isnull(df["Extr Min Temp (°C)"]):
        df['Extr Min Temp (°C)'] = month['Extr Min Temp (°C)']

    if pd.isnull(df["Total Rain (mm)"]):
        df["Total Rain (mm)"] = month['Total Rain (mm)']

    if pd.isnull(df["Total Snow (cm)"]):
        df["Total Snow (cm)"] = month['Total Snow (cm)']

    if pd.isnull(df["Total Precip (mm)"]):
        df['Total Precip (mm)'] = month['Total Precip (mm)']
    
    if pd.isnull(df["Snow Grnd Last Day (cm)"]):
        df['Snow Grnd Last Day (cm)'] = month['Snow Grnd Last Day (cm)']
    
    return df

def prep_monthly_data(weather_list): #NOT DONE
    if weather_list["MLY First Year"] == 0 or weather_list["MLY Last Year"] == 0: #only need the first year
        return

    if os.path.isdir("{}/monthly".format(weather_list["weather_dir"])):
        weather = spark.read.csv("{}/monthly".format(weather_list["weather_dir"]), monthly_weather_schema)
        filtered_monthly = weather.filter(weather['Date/Time'].like("%-%"))\
                                  .select('Date/Time','Year','Month', 
                                          "Mean Max Temp (°C)", "Mean Min Temp (°C)", "Mean Temp (°C)",
                                          'Extr Min Temp (°C)', 'Extr Max Temp (°C)',
                                          "Total Rain (mm)", "Total Snow (cm)", 
                                          "Total Precip (mm)", "Snow Grnd Last Day (cm)")
        
        filtered_monthly = filtered_monthly.toPandas().reset_index(drop=True)
                                           
        # Parse missing dates 
        # Adapted from: https://stackoverflow.com/questions/34326546/reindex-to-add-missing-dates-to-pandas-dataframe

        filtered_monthly['Date/Time'] = pd.to_datetime(filtered_monthly['Date/Time'],format="%Y-%m",errors='coerce')
        filtered_monthly = filtered_monthly[pd.notnull(filtered_monthly['Date/Time'])] # Had a bad 'Date/Time' value 
        
        filtered_monthly = filtered_monthly.sort_values(['Year', 'Month'], ascending=[True, True])
        idx = pd.date_range(filtered_monthly['Date/Time'].iloc[0],filtered_monthly['Date/Time'].iloc[-1])
        
        filtered_monthly = filtered_monthly.set_index("Date/Time")
        filtered_monthly.index = pd.DatetimeIndex(filtered_monthly.index)
        filtered_monthly = filtered_monthly.reindex(idx)        
        
        filtered_monthly['Year'] = filtered_monthly.index.year
        filtered_monthly['Month'] = filtered_monthly.index.month
        
        filtered_monthly = filtered_monthly.sort_values(['Year', 'Month'], ascending=[True, True])
        filtered_monthly = filtered_monthly.reset_index(drop=False)
        filtered_monthly = filtered_monthly.rename(columns={'index':"Date/Time"})

        filtered_monthly['Date/Time'] = pd.to_datetime(filtered_monthly['Date/Time'],format="%Y-%m",errors='coerce')
        filtered_monthly = filtered_monthly[pd.notnull(filtered_monthly['Date/Time'])] # Had a bad 'Date/Time' value 

        filtered_monthly['Date/Time'] = filtered_monthly['Date/Time'].dt.to_period('M')

        filtered_monthly = filtered_monthly.drop_duplicates(['Date/Time'])


        # # Adapted From: https://stackoverflow.com/questions/27905295/how-to-replace-nans-by-preceding-values-in-pandas-dataframe
        # Fill forward a year by grouping months : e.g. 2018 January values -> 2019 January values
        try:
            filtered_monthly = filtered_monthly.sort_values(['Year', 'Month'], ascending=[True, True])\
                                            .groupby(['Month'], as_index=False)\
                                            .fillna(method='ffill', limit=1)\
                                            .reset_index(drop=True)

            # Fill forward a month 
            filtered_monthly = filtered_monthly.sort_values(['Year', 'Month'], ascending=[True, True])\
                                               .fillna(method='ffill', limit=1)
        except:
            pass

        month_avgs = filtered_monthly.groupby(["Month"],as_index=False)\
                                    ['Mean Max Temp (°C)', 'Mean Min Temp (°C)',
                                    'Mean Temp (°C)', 'Extr Max Temp (°C)',
                                    'Extr Min Temp (°C)', 'Total Rain (mm)',
                                    'Total Snow (cm)', 'Total Precip (mm)',
                                    'Snow Grnd Last Day (cm)'].mean()
        # fill rest of the NULLs w/ avg
        filtered_monthly = filtered_monthly.apply(month_avg_filling_nulls,month_avgs=month_avgs,axis=1)

        filtered_monthly = filtered_monthly.set_index('Date/Time').to_csv('./data/consolidated_weather_data/{}-monthly.csv'.format(weather_list["Station ID"]))

def daily_avg_filling_nulls(df, month_avgs):
    month = month_avgs[month_avgs['Month'] == df['Month']]
    month = month.iloc[0]

    if pd.isnull(df["Max Temp (°C)"]):
        df["Max Temp (°C)"] = month["Max Temp (°C)"]
    
    if pd.isnull(df['Min Temp (°C)']):
        df['Min Temp (°C)'] = month['Min Temp (°C)']
        
    if pd.isnull(df["Mean Temp (°C)"]):
        df['Mean Temp (°C)'] = month['Mean Temp (°C)']

    if pd.isnull(df["Total Rain (mm)"]):
        df["Total Rain (mm)"] = month["Total Rain (mm)"]

    if pd.isnull(df["Total Snow (cm)"]):
        df["Total Snow (cm)"] = month["Total Snow (cm)"]

    if pd.isnull(df["Total Precip (mm)"]):
        df['Total Precip (mm)'] = month['Total Precip (mm)']
    
    if pd.isnull(df["Snow on Grnd (cm)"]):
        df['Snow on Grnd (cm)'] = month['Snow on Grnd (cm)']
    
    return df

def consolidate_daily_weather(weather_list):
    if weather_list["DLY First Year"] == 0 or weather_list["DLY Last Year"] == 0:
        return

    if os.path.isdir("{}/daily".format(weather_list["weather_dir"])):
        weather = spark.read.csv("{}/daily".format(weather_list["weather_dir"]), daily_weather_schema)
        filtered_weather = weather.filter(weather['Date/Time'].like("%-%-%"))\
                                .select('Date/Time','Year','Month','Day',
                                        "Max Temp (°C)", "Min Temp (°C)","Mean Temp (°C)", 
                                        "Total Rain (mm)", "Total Snow (cm)", 
                                        "Total Precip (mm)", "Snow on Grnd (cm)")
        
        filtered_weather = filtered_weather.toPandas().reset_index(drop=True)
                    
        try:
            filtered_weather['Date/Time'] = pd.to_datetime(filtered_weather['Date/Time'],format="%Y-%m-%d",errors='coerce')
            # Adapted From: https://stackoverflow.com/questions/13413590/how-to-drop-rows-of-pandas-dataframe-whose-value-in-a-certain-column-is-nan
            filtered_weather = filtered_weather[pd.notnull(filtered_weather['Date/Time'])] # Had a bad 'Date/Time' value 
            # print('{}:\n{}'.format(filtered_weather['Date/Time'],weather_list['temp_name']))
            # Adapted from: https://stackoverflow.com/questions/34326546/reindex-to-add-missing-dates-to-pandas-dataframe
            filtered_weather = filtered_weather.sort_values(['Date/Time','Year', 'Month', 'Day'], ascending=[True,True, True, True])
            idx = pd.date_range(filtered_weather['Date/Time'].iloc[0],filtered_weather['Date/Time'].iloc[-1])
            
            filtered_weather = filtered_weather.set_index("Date/Time")   
            filtered_weather.index = pd.DatetimeIndex(filtered_weather.index)
            filtered_weather = filtered_weather.reindex(idx)

            filtered_weather['Year'] = filtered_weather.index.year
            filtered_weather['Month'] = filtered_weather.index.month
            filtered_weather['Day'] = filtered_weather.index.day
            
            filtered_weather = filtered_weather.sort_values(['Year', 'Month', 'Day'], ascending=[True, True, True])
            filtered_weather = filtered_weather.reset_index(drop=False)
            filtered_weather = filtered_weather.rename(columns={'index':"Date/Time"})

            filtered_weather = filtered_weather[(filtered_weather['Date/Time'] < '2019-07-16')]
        except:
            pass
        
        # Cut out data where it has not arrived yet and prior to set time
        # filtered_weather['Date/Time'] = pd.to_datetime(filtered_weather['Date/Time'])

        # Adapted From: https://stackoverflow.com/questions/27905295/how-to-replace-nans-by-preceding-values-in-pandas-dataframe
        # Take prev yrs weather and fill into null -> e.g. jan 01 2018 -> jan 01 2019
        try:
            filtered_weather = filtered_weather.sort_values(['Year', 'Month', 'Day'], ascending=[True, True, True])\
                                            .groupby(['Month', 'Day'], as_index=False)\
                                            .fillna(method='ffill', limit=1)\
                                            .reset_index(drop=True)
            # Fill data from previous days data
            filtered_weather = filtered_weather.sort_values(['Year', 'Month', 'Day'], ascending=[True, True, True])\
                                            .fillna(method='ffill',limit=1)
        except:
            pass    
        
        month_avgs = filtered_weather.groupby(["Month", "Day"],as_index=False)\
                                            ['Max Temp (°C)', 'Min Temp (°C)',
                                             'Mean Temp (°C)', 'Total Rain (mm)', 
                                             'Total Precip (mm)','Total Snow (cm)', 
                                             'Snow on Grnd (cm)'].mean()

        #take avg and fill remaining nulls
        filtered_weather = filtered_weather.apply(daily_avg_filling_nulls,month_avgs=month_avgs,axis=1)
        filtered_weather = filtered_weather.set_index("Date/Time").to_csv('./data/consolidated_weather_data/{}-daily.csv'.format( weather_list["Station ID"] ))
        # .sort_values(['Year', 'Month', 'Day'], ascending=[True, True, True])\




weather_fpath = "./index_data/closest_weather_to_hydro_stations.csv"
weather_inventory = pd.read_csv(weather_fpath, sep=",")

weather_inventory = weather_inventory[['Name', 'Station ID', 'First Year', 'Last Year',
                                        "DLY First Year","DLY Last Year","MLY First Year","MLY Last Year"]].drop_duplicates()

weather_inventory["temp_name"] = weather_inventory.apply(clean_name, axis=1)
weather_inventory["weather_dir"] = weather_inventory.apply(get_dir, axis=1)

weather_inventory = weather_inventory.sort_values(['Name'],ascending=[True])
weather_inventory.apply(prep_monthly_data,axis=1)
weather_inventory.apply(consolidate_daily_weather,axis=1)


