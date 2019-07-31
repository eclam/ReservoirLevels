import pandas as pd
from pandas.io.json import json_normalize

df = pd.read_json("ENVCAN_HYDROMETRIC_STN_SP.geojson",)
df = df[["features"]]
df = json_normalize(df['features'])

# lon then lat for coordinates

# Adapted From: https://stackoverflow.com/questions/35491274/pandas-split-column-of-lists-into-multiple-columns
df[['longitude', 'latitude']] = pd.DataFrame(
    df['geometry.coordinates'].values.tolist(), index=df.index)


df = df[["properties.STATION_NUMBER", "properties.STATION_NAME", "properties.HYDROMETRIC_STATION_ID",
         "properties.STATION_OPERATING_STATUS", "latitude", "longitude",
         "properties.END_DATE", "properties.START_DATE",
         "properties.ARCHIVE_URL", "properties.REALTIME_URL",
         "properties.OBJECTID", "properties.FEATURE_CODE"]]

df.rename(columns={"properties.STATION_NUMBER": "station_number",
                   "properties.STATION_NAME": "station_name",
                   "properties.HYDROMETRIC_STATION_ID": "station_id",
                   "properties.STATION_OPERATING_STATUS": "operating_status",
                   "geometry.coordinates": "coordinates",
                   "properties.END_DATE": "end_date",
                   "properties.START_DATE": "start_date",
                   "properties.ARCHIVE_URL": "archive_url",
                   "properties.REALTIME_URL": "realtime_url",
                   "properties.OBJECTID": "object_id",
                   "properties.FEATURE_CODE": "feature_code"
                   }, inplace=True)

df = df[df['operating_status'].str.contains("ACTIVE")]
# df = df[df['operating_status'] == "ACTIVE-REALTIME"]
df = df.drop(['operating_status'], axis=1)

# Adpated from: https://stackoverflow.com/questions/15891038/change-data-type-of-columns-in-pandas
df[['start_date', 'end_date']] = df[['start_date','end_date']].apply(pd.to_numeric) # get proper years

df = df[(df['start_date']<= 20001231) & (df['end_date']>= 20150101)]
df = df.sort_values(['station_name'], axis=0).reset_index(drop=True)


# print(df[df['station_name'].str.contains("SEYM")])  # Search command
df.to_csv("hydro_stn_active.csv")

print("WARNING: deprecated b/c archive got deprecated by Canadian govt! This data is in Hydat.sqlite3 anyway.")