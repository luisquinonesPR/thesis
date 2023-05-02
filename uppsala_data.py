#%%
# credit to this helpful repository: https://github.com/UppsalaConflictDataProgram/basic_api_recipes
import requests
import pandas as pd

#pip install fiona
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import requests
from io import BytesIO
import shutil
import zipfile
#import osx

path = os.getcwd()
path

# %%
# OVERVIEW

# This notebook combines several UCDP data sets and performs exploratoory data analysis.
# All combined data sets have the same columns.
# 1) One is the UCDP GED data set from 1989-2021. The latest version of UCDP GED: 22.1
#    This code inlcudes a way of obtaining the data via the API, but this takes a while.
#    It is better to instead download the csv from here and store it in the data folder: https://ucdp.uu.se/downloads/index.html#ged_global
# 2) The later data set are much smaller and can thus be loaded via the API code.
#    These are so-called "candidate events"
#    "The main difference between UCDP Candidate and UCDP GED is that UCDP Candidate relaxes some of UCDPs criteria
#     for inclusion in order to release the data early without having extra time to research an event." (https://ucdp.uu.se/downloads/candidateged/ucdp-candidate-codebook1.3.pdf)

#%%
#####################################################################################################################
# Data set 1
#####################################################################################################################

# Import and inspect UCDP GED data set (1989-2021) via csv
df1 = pd.read_csv(path + '/data/GEDEvent_v22_1.csv')

#%%
print(df1.shape)
#print(df1.columns)
print(df1.date_start.min())
print(df1.date_start.max())
df1.head()

#%%
## SKIP THIS AS TAKES LONGER
# Import and inspect UCDP GED data set (1989-2021) via API

api_url = 'https://ucdpapi.pcr.uu.se/api/gedevents/22.1' 
params = {
    'pagesize': 1000,
    'page': 0
}
response = requests.get(api_url, params=params)
print(response.status_code)

#%%%

# Check if request was successful
if response.status_code == 200:
    json_data = response.json()
    
    # Get the total number of pages
    total_pages = json_data['TotalPages']
    
    # Extract the 'Result' array from the JSON response
    data = json_data['Result']

    # Convert JSON data to a pandas dataframe
    df = pd.DataFrame(data)

    # Iterate through the rest of the pages and append data to the DataFrame
    for page in range(1, total_pages + 1):
        params['page'] = page
        response = requests.get(api_url, params=params)
        json_data = response.json()
        data = json_data['Result']
        temp_df = pd.DataFrame(data)
        df = pd.concat([df, temp_df], ignore_index=True)
        
        # Print progress
        print(f"Page {page} of {total_pages} fetched")

    # Save the DataFrame to a CSV file
    df.to_csv('ucdp_api_to_dataframe.csv', index=False)

    # Display the dataframe
    print(df)
else:
    print(f"Error: {response.status_code}. {response.text}")

#%%
print(df.shape)
#print(df.columns)
print(df.date_start.min())
print(df.date_start.max())
#df.iloc[3000]
df.head()


# %%
#####################################################################################################################
# Data set 2
#####################################################################################################################

# Import and inspect UCDP Candidate data sets (2022-2023) via API
# January to December 2022: Version 22.01.22.12
# January 2023: Version 23.0.1
# February 2023: Version 23.0.2 
# March 2023: Version 23.0.3 


link_prefix = 'https://ucdpapi.pcr.uu.se/api/gedevents/'
link_suffixes = ['23.0.3', '23.0.2', '23.0.1', '22.01.22.12']

api_urls = []

for suffix in link_suffixes:
    full_link = link_prefix + str(suffix)
    api_urls.append(full_link)

print(api_urls)

params = {
    'pagesize': 1000,
    'page': 0
}
#response = requests.get(api_url2, params=params)
#print(response.status_code)

#%%
for url in api_urls:
    response = requests.get(url, params=params)

    # Check if request was successful
    if response.status_code == 200:
        json_data = response.json()
        
        # Get the total number of pages
        total_pages = json_data['TotalPages']
        
        # Extract the 'Result' array from the JSON response
        data = json_data['Result']

        # Convert JSON data to a pandas dataframe
        df2 = pd.DataFrame(data)

        # Iterate through the rest of the pages and append data to the DataFrame
        for page in range(1, total_pages + 1):
            params['page'] = page
            response = requests.get(url, params=params)
            json_data = response.json()
            data = json_data['Result']
            temp_df = pd.DataFrame(data)
            df2 = pd.concat([df2, temp_df], ignore_index=True)
            
            # Print progress
            print(f"Page {page} of {total_pages} fetched")

        # Save the DataFrame to a CSV file
        df2.to_csv('ucdp_api_to_dataframe.csv', index=False)

        # Display the dataframe
        print(df2)
    else:
        print(f"Error: {response.status_code}. {response.text}")

#%%
print(df2.shape)
#print(df_new.shape)

#print(df2.columns)
print(df2.date_start.min())
print(df2.date_start.max())
#df.iloc[3000]
df2.head()


# %%

#####################################################################################################################
# Data sets combined 
#####################################################################################################################

#check they have the same column headings
print(set(df1.columns) - set(df2.columns))

#%%
# Combine UCDP GED data set (1989-2021) and UCDP Candidate data sets (2022-2023)
df_all = pd.concat([df1, df2], ignore_index=True)

#%%
print(df_all.shape)
#print(df_all.columns)
print(df_all.date_start.min())
print(df_all.date_start.max())

# %%

#####################################################################################################################
# EDA
#####################################################################################################################

# Overview of missingness
print(df_all.isna().sum())

# Overview of unique values
df_all.agg(['size', 'count', 'nunique']).T #difference between size and count are the missing values

# %%
# conflicts per year
df_all.year.value_counts().sort_index(axis=0).plot.bar()
plt.show()

#%%

#conflict regions
df_all.region.value_counts().plot.bar()
plt.show()

#country
plt.rcParams["figure.figsize"] = (20,6)
df_all.country.value_counts().plot.bar()
plt.show()

#type_of_violence Type of UCDP conflict:
#1: state-based conflict
#2: non-state conflict
#3: one-sided violence
plt.rcParams["figure.figsize"] = (8,6)
df_all.type_of_violence.value_counts().plot.bar()
plt.show()

# where_prec: "The precision with which the coordinates and location assigned to the event reflects the location of the actual event."
# from "1: exact location of the event known and coded." to
# "6: only the country where the event took place in is known." and "7: event in international waters or airspace."
df_all.where_prec.value_counts().plot.bar()
plt.show()

#active_year:
#1: if the event belongs to an active conflict/dyad/actoryear
#0: otherwise 
df_all.active_year.value_counts().plot.bar()
plt.show()

# Most entries are "clear" - this code_status variable is not even inlcuded in the codebook for the GED data set
# "Clear is used when we know what parties are involved, that the actors are organized enough (see definition of organized actor),
# that there is a reported number of deaths,that we trust the source, that we have geo-coded the location, and for state-based
# violence that we have identified a stated incompatibility. The clear events are included
# in UCDP GED if the dyad has ever reached the 25-fatality threshold for inclusion."
df_all.code_status.value_counts().plot.bar()
plt.show()


#%%

#event_clarity
df_all.event_clarity.value_counts().plot.bar()
plt.show()

#date_prec
df_all.date_prec.value_counts().plot.bar()
plt.show()


# %%


# %%
