import pandas as pd
import requests
from apikey import apikey 

# load and transform 2019 data
df_soil_19 = pd.read_excel("2019_Metals_Results_-_Soil_Kitchen.xls")

df_soil_19 = df_soil_19.drop(columns=["Unnamed: 0"])

df_soil_19.columns = df_soil_19.iloc[0]

df_base_19 = df_soil_19.head()

df_soil_19 = df_soil_19.drop(index=[0, 1, 2, 3, 4])

df_soil_19 = df_soil_19[["Street", "Cross Street", "Lead", "Arsenic"]]

# Load and transform 2015-2018 data
df_soil_18_15 = pd.read_csv("2015-18 Soil_Kitchen_Log.csv")

df_soil_18_15 = df_soil_18_15[
    ["Street", "Cross Street", "Full Address", "Lead", "Arsenic"]
]

def request_geocoder(intersection, apikey, city="Austin, TX"):
    """returns lat. lon for cross streets in street1@street2 format.
    Defailt city is Austin, TX and apikey is required"""

    base_url = "https://geocoder.ls.hereapi.com/6.2/geocode.json"
    parameters = {"city": city, "street": intersection, "gen": 9, "apiKey": apikey}
    try:
        response = requests.get(base_url, params=parameters)
        j_resp = response.json()
        lat = j_resp["Response"]["View"][0]["Result"][0]["Location"]["DisplayPosition"][
            "Latitude"
        ]
        lon = j_resp["Response"]["View"][0]["Result"][0]["Location"]["DisplayPosition"][
            "Longitude"
        ]
    #         print(parameters['street'], 'success')
    except:
        lat = ""
        lon = ""
        print(parameters["street"], response)

    return pd.Series([lat, lon], index=["Lat", "Lon"])

    def query_cross_streets(df):
    """this function uses geocoder.ls.hereapi.com to find the geocodes for cross streets from a dataframe"""
    df[["Lat", "Lon"]] = df.apply(
        lambda x: request_geocoder(x["Intersection"], apikey), axis=1
    )
    return df

    df_soil_18_15["Lead"] = pd.to_numeric(df_soil_18_15["Lead"], errors="coerce")

df_soil_18_15["Arsenic"] = pd.to_numeric(df_soil_18_15["Arsenic"], errors="coerce")

df_soil = df_soil_19.append(df_soil_18_15, ignore_index=True)

df_soil["Intersection"] = df_soil.apply(
    lambda x: str(x["Street"]) + "@" + str(x["Cross Street"]), axis=1
)

df_soil[["Lead", "Arsenic"]] = df_soil[["Lead", "Arsenic"]].fillna(0)

df_soil_mean = (
    df_soil[["Intersection", "Lead", "Arsenic"]]
    .groupby(["Intersection"])
    .mean()
    .reset_index()
)

# geocode data
df_soil_mean_loc = query_cross_streets(df_soil_mean)
df_soil_mean_loc = df_soil_mean_loc.dropna()
df_soil_mean_loc.to_csv('df_soil_mean_loc.csv')