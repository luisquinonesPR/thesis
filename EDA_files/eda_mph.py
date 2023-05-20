#%%

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import pycountry

import os

path = os.getcwd()
parent_path = os.path.abspath(os.path.join(path, os.pardir))
parent_path
# %%
##########################################################################
# Importing data: merged and GDELT original (admin1)
##########################################################################

#############
# UCDP
#############

until_2021 = pd.read_csv(os.path.abspath(parent_path + '/data/GEDEvent_v22_1.csv'))
candidates = pd.read_csv(os.path.abspath(parent_path + '/data/candidates_120523.csv'))

ucdp_og = pd.concat([until_2021, candidates], ignore_index=True)
ucdp_og.head()
#%%
ucdp = ucdp_og.copy()

#############
# MERGED
#############

df_merged_og = pd.read_csv(os.path.abspath(parent_path + '/data/merged.csv'))
df_merged_og.head()
# %%
df_merged = df_merged_og.copy()
# %%
print(df_merged.isocode.nunique())
df_merged.isocode.unique()
#%%
df_merged.columns
#%%

print(df_merged.MonthYear.min())
print(df_merged.MonthYear.max())

#############
# GDELT
#############

gdelt_og = pd.read_csv(os.path.abspath(parent_path + '/data/final_gdelt_bycountry.txt'))
gdelt_og.head()
#%%
gdelt = gdelt_og.copy()
#%%
gdelt['MonthYear'] = gdelt.apply(lambda row: str(row['year']) + str(row['month']).zfill(2), axis=1)

gdelt.columns
#%%
print(gdelt.isocode.nunique())
gdelt.isocode.unique()

#%%

#############
# ISO to fips
#############

file_path = os.path.abspath(parent_path + '/data/country_codes_fips_to_iso3c.txt') 
with open(file_path, 'r') as file:
    lines = file.readlines()
    row_count = len(lines)

print("Number of rows:", row_count)


# %%
##########################################################################
# EDA on GDELT DATA
##########################################################################

print(gdelt['year'].min())
print(gdelt['year'].max())

gdelt = gdelt[gdelt['year'] > 1979]

gdelt.head()


#%%
gdelt['ActionGeo_ADM1Code']

#%%
get_isocode('Angola') # AGO
get_isocode('Brazil') #BRA

#%%
grouped_admin_overview = gdelt.groupby('isocode')['ActionGeo_ADM1Code'].nunique(dropna = False).reset_index() #(name='count')

grouped_admin_overview.head()
grouped_admin_overview.describe().applymap(lambda x: f"{x:0.2f}")


#%%
grouped_admin1_detail = gdelt.groupby(['isocode', 'ActionGeo_ADM1Code']).size().reset_index(name='count')
grouped_admin1_detail

print(len(grouped_admin1_detail[grouped_admin1_detail['isocode']=='AGO']))
print(grouped_admin1_detail[grouped_admin1_detail['isocode']=='AGO'])


#%%
rouped_admin1_detail = gdelt.groupby(['isocode', 'ActionGeo_ADM1Code']).size().reset_index(name='count')
grouped_admin1_detail

print(len(grouped_admin1_detail[grouped_admin1_detail['isocode']=='BRA']))
print(grouped_admin1_detail[grouped_admin1_detail['isocode']=='BRA'])


#%%
unique_isocodes_per_year_gdelt = gdelt.groupby('year')['isocode'].nunique()

unique_isocodes_per_monthyear_gdelt = gdelt.groupby('MonthYear')['isocode'].nunique()

# Plotting the number of unique isocodes per year
plt.figure(figsize=(10, 5))
plt.bar(unique_isocodes_per_year.index, (240-unique_isocodes_per_year.values))
plt.xlabel('Year')
plt.ylabel('Difference from maximum')
plt.title('Number of Countries Missing (out of 240) per Year')
plt.show()

# Plotting the number of unique isocodes per MonthYear
plt.figure(figsize=(10, 5))
plt.bar(unique_isocodes_per_monthyear.index, (240-unique_isocodes_per_monthyear.values))
plt.xlabel('MonthYear')
plt.ylabel('Number of Unique Isocodes')
plt.title('Number of Countries per MonthYear')
plt.xticks(rotation=45)
plt.show()


# %%
##########################################################################
# EDA on MERGED DATA
##########################################################################
# Remove 1920 outliers
df_merged = df_merged[df_merged['year'] > 1979]

#%%

# Missing countries in different time periods

# Count unique isocodes per year
unique_isocodes_per_year = df_merged.groupby('year')['isocode'].nunique()

# Count unique isocodes per MonthYear
unique_isocodes_per_monthyear = df_merged.groupby('MonthYear')['isocode'].nunique()

unique_isocodes_per_monthyear.head()
#%%
# Plotting the number of unique isocodes per year
plt.figure(figsize=(10, 5))
plt.bar(unique_isocodes_per_year.index, (240-unique_isocodes_per_year.values))
plt.xlabel('Year')
plt.ylabel('Difference from maximum')
plt.title('Number of Countries Missing per Year')
plt.show()

# Plotting the number of unique isocodes per MonthYear
plt.figure(figsize=(10, 5))
plt.bar(unique_isocodes_per_monthyear.index, (240 - unique_isocodes_per_monthyear.values))
plt.xlabel('MonthYear')
plt.ylabel('Difference from maximum')
plt.title('Number of Countries Missing per MonthYear')
plt.xticks(rotation=45)
plt.show()

#%%

## NUMBER OF GDELT ENTRIES PER COUNTRY and year (unique MonthYear)

df_merge_pre_2023 = df_merged[df_merged['year'] < 2023]
df_merge_pre_2023.head()
#%%

country_time_periods = df_merge_pre_2023.groupby(['isocode', 'year'])['MonthYear'].nunique()
country_time_periods.reset_index()
country_time_periods_df = country_time_periods.to_frame()
country_time_periods_df

#%%
min_med_df = country_time_periods_df.groupby('isocode')['MonthYear'].agg(['min','median']) #'max', 'mean', 
min_med_df.reset_index()
min_med_df.head()

#%%
min_med_df[['min','median']].plot(kind='bar', fontsize=6, figsize=(20, 10))

plt.xlabel('Country')
plt.ylabel('Value')
plt.title('Min and Median Number of Annual Months for Each Country')
plt.show()

#%%
year_counts = df_merge_pre_2023.groupby('isocode')['MonthYear'].nunique()
year_counts.reset_index()
year_counts_df = year_counts.to_frame()
year_counts_df.reset_index(inplace=True)

year_counts_df.head()
#%%
print(year_counts_df['MonthYear'].max()) # 516 - which is in line with 43 years * 12 months

year_counts_df.head()
missing_months = year_counts_df[year_counts_df['MonthYear']<413] # 413 is 80% of possible values

missing_months.shape

#%%
def get_country_name(isocode):
    try:
        country = pycountry.countries.get(alpha_3=isocode)
        return country.name
    except LookupError:
        return None

def get_isocode(country_name):
    try:
        country = pycountry.countries.search_fuzzy(country_name)[0]
        return country.alpha_3
    except LookupError:
        return None

#%%

missing_months['country'] = missing_months['isocode'].apply(get_country_name)

#%%%
hi_miss_country = missing_months['country'].tolist()
print(hi_miss_country)



#%%

events_colums = ['count_events_1', 'count_events_2', 'count_events_3', 'count_events_4', 'count_events_5',
                  'count_events_6', 'count_events_7', 'count_events_8', 'count_events_9', 'count_events_10',
                  'count_events_11', 'count_events_12', 'count_events_13', 'count_events_14', 'count_events_15',
                  'count_events_16', 'count_events_17', 'count_events_18', 'count_events_19', 'count_events_20']

# Create the new column by summing the values across the specified columns
df_merged['sum_of_events'] = df_merged[events_colums].sum(axis=1)


#%%



# Group by year and calculate the total sum and range for each variable
events_each_year = df_merged.groupby('year')[events_colums].agg(['sum', 'min', 'max'])

# Flatten the multi-level column names
events_each_year.columns = [f"{var}_{agg}" for var in events_colums for agg in ['sum', 'min', 'max']]

# Display the grouped dataframe
events_each_year.columns

#%%

sum_variables = [var for var in events_each_year.columns if var.endswith('_sum')]
sum_variables

#%%

events_each_year[sum_variables].plot(kind='bar', stacked=True, fontsize=9, legend=False)



# %%
##########################################################################
# EDA on UCDP - AR model
##########################################################################

ucdp.head()
ucdp.columns
# %%
