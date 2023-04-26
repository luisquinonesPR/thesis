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

os.getcwd()

#%%

df1 = pd.read_csv()

#%%


api_url = 'https://ucdpapi.pcr.uu.se/api/gedevents/22.1' 
params = {
    'pagesize': 1000,
    'page': 0
}
response = requests.get(api_url, params=params)
print(response.status_code)


# %%

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

# %%
print(df.shape)
print(df.columns)
print(df.date_start.min())
print(df.date_start.max())
#df.iloc[3000]

# %%
df.head()

# %%

# first tried with this, but it only contains a single month of the candidate data
#api_url2 = 'https://ucdpapi.pcr.uu.se/api/gedevents/23.0.3' 

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
        df_new = pd.DataFrame(data)

        # Iterate through the rest of the pages and append data to the DataFrame
        for page in range(1, total_pages + 1):
            params['page'] = page
            response = requests.get(api_url2, params=params)
            json_data = response.json()
            data = json_data['Result']
            temp_df = pd.DataFrame(data)
            df_new = pd.concat([df_new, temp_df], ignore_index=True)
            
            # Print progress
            print(f"Page {page} of {total_pages} fetched")

        # Save the DataFrame to a CSV file
        df2.to_csv('ucdp_api_to_dataframe.csv', index=False)

        # Display the dataframe
        print(df_new)
    else:
        print(f"Error: {response.status_code}. {response.text}")

#%%

print(df2.shape)
print(df2.columns)
print(df2.date_start.min())
print(df2.date_start.max())
#df.iloc[3000]

#%%
#%%

print(df_new.shape)
print(df_new.columns)
print(df_new.date_start.min())
print(df_new.date_start.max())

# %%
df.head()

# %%
df_all = pd.concat([df, df2], ignore_index=True)

#%%


print(df_all.shape)
print(df_all.columns)
print(df_all.date_start.min())
print(df_all.date_start.max())



# %%
