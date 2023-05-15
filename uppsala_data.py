#%%
# credit to this helpful repository: https://github.com/UppsalaConflictDataProgram/basic_api_recipes
import requests
import pandas as pd

#pip install fiona
import numpy as np
import pandas as pd
#import geopandas as gpd
import matplotlib.pyplot as plt
import requests
from io import BytesIO
import shutil
import zipfile
import os
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

df1[['date_end', 'date_start']].head()

#%%
df1[df1['country'] == 'Syria']


# %%
#####################################################################################################################
# Data set 2
#####################################################################################################################

#Getting data from data folder

df2 = pd.read_csv(path + '/data/candidates_120523.csv')

#%%
print(df2.shape)
#print(df1.columns)
print(df2.date_start.min())
print(df2.date_start.max())
df2.head()

df2[['date_end', 'date_start']].head()

# %%
#####################################################################################################################
# Data sets combined 
#####################################################################################################################

#check they have the same column headings
print(set(df1.columns) - set(df2.columns))

#%%
# Combine UCDP GED data set (1989-2021) and UCDP Candidate data sets (2022-2023)
df_all = pd.concat([df1, df2], ignore_index=True)

#df_all = df1.copy()
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
df_all.isna().sum().T

#%%
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

#number_of_sources
df_all.number_of_sources.value_counts().plot.bar()


# %%

# where_prec: "The precision with which the coordinates and location assigned to the event reflects the location of the actual event."
# from "1: exact location of the event known and coded." to
# "6: only the country where the event took place in is known." and "7: event in international waters or airspace."
df_all.where_prec.value_counts().plot.bar()
plt.show()

# location description
df_all['where_description']	


#%%
# DEATHS PER YEAR (and by type of violence)

deaths_annual = df_all.groupby('year')[['best_state', 'best_nonstate', 'best_onesided']].sum()
deaths_annual.head()
#%%
deaths_annual.plot(kind='bar', stacked=True, figsize=(10,6))
plt.xlabel('Year')
plt.ylabel('Total Amount')
plt.title('Total Number of Deaths by Year')
plt.show()

#%%
# Rwanda acccounts for 1994 outlier
df_all[(df_all['best']>10000) & (df_all['type_of_violence']==3)]

#%%
# CONFLICT EVENTS PER YEAR (and by type of violence)

confl_events_annual = df_all.groupby('year')['type_of_violence'].value_counts().unstack(fill_value=0)
confl_events_annual = confl_events_annual.reset_index()
confl_events_annual.columns = ['year', 'type_1', 'type_2', 'type_3']
confl_events_annual = confl_events_annual.set_index('year')
confl_events_annual.head()
#%%
confl_events_annual.plot(kind='bar', stacked=True)
plt.xlabel('Year')
plt.ylabel('Count')
plt.title('Count of Each Type of Violence per Year')
plt.show()

#%%

confl_regions_annual = df_all.groupby('year')['region'].value_counts().unstack(fill_value=0)

confl_regions_annual = confl_regions_annual.reset_index()
confl_regions_annual.columns = ['year', 'Africa', 'Americas', 'Asia', 'Europe', 'Middle East']
confl_regions_annual = confl_regions_annual.set_index('year')

confl_regions_annual.head()

#%%
confl_regions_annual.plot(kind='bar', stacked=True)
plt.xlabel('Year')
plt.ylabel('Count')
plt.title('Breakdown of events by Region per Year')
plt.show()


#%% 


#%%
# Inspecting administration levels available per country

# We group them without dropping nan values
grouped_admin_overview = df_all.groupby('country')[['adm_1', 'adm_2']].nunique(dropna = False).reset_index() #(name='count')

# There are a lot of missing values for the admin layers
print('Missing values')
print(df_all[['adm_1', 'adm_2', 'country']].isna().sum())
print(grouped_admin_overview[['adm_1', 'adm_2', 'country']].isna().sum())


print(grouped_admin_overview.head(20))
grouped_admin_overview.describe().applymap(lambda x: f"{x:0.2f}")


#%%
# There are 127 countries in both the overall and the grouped data frame
countries_all = df_all['country'].unique()
countries_grouped = grouped_admin_overview['country'].unique()

print('Number of countries in df:' + str(len(countries_all)))
print('Number of countries in groupby: ' + str(len(countries_grouped)))

# this issue has now been resolved
# for some reason the grouped table does not include {'Belarus', 'Dominican Republic', 'Poland', 'Sao Tome and Principe'}
print('Countries missing from groupby')
print(set(countries_all) - set(countries_grouped))

# although inspecting further, only Belarus's event has no adm data
#no_data_counrties = ['Belarus', 'Dominican Republic', 'Poland', 'Sao Tome and Principe']

#for i in range(len(no_data_counrties)):
#    print(df_all[df_all['country']==no_data_counrties[i]][['country','adm_1', 'adm_2']])

#%%




#%%
# Inspecting adm1 (e.g. province) and adm2 (e.g. municipality)
grouped_admin1_detail = df_all.groupby(['country', 'adm_1']).size().reset_index(name='count')
grouped_admin2_detail = df_all.groupby(['country', 'adm_2']).size().reset_index(name='count')

print(grouped_admin1_detail.country.nunique())
print(grouped_admin1_detail.country.nunique(dropna=False))

print(grouped_admin2_detail.country.nunique())
print(grouped_admin2_detail.country.nunique(dropna=False))

#%%
# exmples for Angola: 18 adm1 and 149 adm2
print(len(grouped_admin1_detail[grouped_admin1_detail['country']=='Angola']))

print(grouped_admin1_detail[grouped_admin1_detail['country']=='Angola'])
#print(grouped_admin2_detail[grouped_admin2_detail['country']=='Angola'])

#%%
# exmples for Brazil
print(len(grouped_admin1_detail[grouped_admin1_detail['country']=='Brazil']))

print(grouped_admin1_detail[grouped_admin1_detail['country']=='Brazil'])
#print(grouped_admin2_detail[grouped_admin2_detail['country']=='Angola'])


#%%
# exmples for Germany: 
print(grouped_admin1_detail[grouped_admin1_detail['country']=='Germany'])
print(grouped_admin2_detail[grouped_admin2_detail['country']=='Germany'])

df_all[df_all['country']=='Germany'][['id', 'relid', 'year', 'active_year','side_a','side_b' ,'adm_1','adm_2', 'date_start', 'date_end', 'deaths_a',
       'deaths_b', 'deaths_civilians', 'deaths_unknown',]]

#%%
fig, ax = plt.subplots(figsize=(10, 5))
grouped_admin_overview.plot(kind='bar', x='country', y=['adm_1', 'adm_2'], ax=ax)
ax.set_title('Count of unique adm_1 and adm_2 per country')
ax.set_xlabel('Country')
ax.set_ylabel('Count')
plt.show()

# %%

#pd.set_option('display.max_columns', None)

# Creating possible target features:
df_all['best_state'] = np.where(df_all['type_of_violence'] == 1, df_all['best'], np.nan)
df_all['best_nonstate'] = np.where(df_all['type_of_violence'] == 2, df_all['best'], np.nan)
df_all['best_onesided'] = np.where(df_all['type_of_violence'] == 3, df_all['best'], np.nan)


#%%
# Conversion below didn't work for this entry becuase o
df_all.iloc[29363]['date_start']  #'2020-01-06 00:00:00.000' while other had format ''2022-03-09T00:00:00''
#%%
# Converting 'date_start' column to datetime
df_all['date_start'] = pd.to_datetime(df_all['date_start'], format='ISO8601')

#%%
# Fixing the number of sources
df_all['number_of_sources'] = df_all['number_of_sources'].replace(-1, 0)

# Group by month, adm_1, and country and aggregate variables
grouped_df = df_all.groupby([df_all['date_start'].dt.month.rename('month'), df_all['date_start'].dt.year.rename('year'), 'adm_1', 'country', 'country_id', 'region']).agg({
    'best': [('best','sum')],
    'best_state': [('best_state','sum')],
    'best_nonstate': [('best_nonstate','sum')],
    'best_onesided': [('best_onesided','sum')],
    'deaths_civilians': [('deaths_civilians','sum')],
    'number_of_sources': [('number_of_sources','mean')],
    'conflict_new_id': [('count_conflict_new_id', 'nunique'), ('freq_conflict_new_id', lambda x: x.value_counts().index[0])],
    'dyad_new_id': [('count_dyad_new_id', 'nunique'), ('freq_dyad_new_id', lambda x: x.value_counts().index[0])]
}).reset_index()

grouped_df.head()

#%%
grouped_df.columns = [col[1] if isinstance(col, tuple) and col[1] != '' else col[0] for col in grouped_df.columns]

grouped_df.head()
#%%%

grouped_df['MonthYear'] = grouped_df.apply(lambda row: str(row['year']) + str(row['month']).zfill(2), axis=1)
grouped_df.to_csv(path + '/data/ucdp_grouped.csv', index=False)

grouped_df.head(10)

#%%
##################################
# OLD WAY OF GETTING DF1
##################################
## SKIP THIS AS TAKES LONGER
# Import and inspect UCDP GED data set (1989-2021) via API

#api_url = 'https://ucdpapi.pcr.uu.se/api/gedevents/22.1' 
#params = {
#    'pagesize': 1000,
#    'page': 0
#}
#response = requests.get(api_url, params=params)
#print(response.status_code)

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

##################################
# OLD WAY OF GETTING DF2
##################################

#%
# Downloading via API

# Import and inspect UCDP Candidate data sets (2022-2023) via API
# January to December 2022: Version 22.01.22.12
# January 2023: Version 23.0.1
# February 2023: Version 23.0.2 
# March 2023: Version 23.0.3 

#link_prefix = 'https://ucdpapi.pcr.uu.se/api/gedevents/'
# not yet available (on 3 May 23) but should be out soon: '23.0.4'
#link_suffixes = ['23.0.3', '23.0.2', '23.0.1', '22.01.22.12']

#api_urls = []

#for suffix in link_suffixes:
#    full_link = link_prefix + str(suffix)
#    api_urls.append(full_link)

#print(api_urls)

#params = {
#    'pagesize': 1000,
#    'page': 0
#}
#response = requests.get(api_url2, params=params)
#print(response.status_code)

#%%

# Initialise empty dataframe
df_2 = pd.DataFrame()

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
        #df2.to_csv('ucdp_api_to_dataframe.csv', index=False)

        # Display the dataframe
        print(df2)
        # Attache df2 for this url to overall df_2
        df_2 = pd.concat([df2,df_2], ignore_index=True)
    else:
        print(f"Error: {response.status_code}. {response.text}")

#%%
print(df_2.shape)
#print(df_new.shape)

#print(df2.columns)
print(df_2.date_start.min())
print(df_2.date_start.max())
#df.iloc[3000]
df_2.head()

# Export to csv
df_2.to_csv(path + '/data/candidates_120523.csv', index=False)