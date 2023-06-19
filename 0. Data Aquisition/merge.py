#%%
# !pip install pycountry
import pandas as pd
import matplotlib.pyplot as plt
import os
import pycountry
import re

path = os.getcwd()

#%%
# Loading the data
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

ucdp = pd.read_csv(path + '/data/ucdp_grouped.csv')
gdelt = pd.read_csv(path + '/data/final_gdelt_bycountry.txt')

gdelt['year'].min()
gdelt['year'].max()

#%%
# Grouping UCDP by country to merge
ucdp_country = ucdp.groupby(['MonthYear', 'country', 'month', 'year', 'region']).agg(
    deaths = ('best', 'sum'),
    state_deaths = ('best_state','sum'),
    nonstate_deaths = ('best_nonstate','sum'),
    onesided_deaths = ('best_onesided','sum'),
    civilian_deaths = ('deaths_civilians','sum'),
    avg_sources = ('number_of_sources', 'mean'), 
    conflict_counts = ('count_conflict_new_id', 'sum'),
    conflict_freq = ('freq_conflict_new_id', lambda x: x.value_counts().index[0]),
    dyad_counts = ('count_dyad_new_id', 'sum'),
    dyad_freq = ('freq_dyad_new_id', lambda x: x.value_counts().index[0])).reset_index()

ucdp_country.head(10)

# Regex to make merging easier

ucdp_country['country'] = ucdp_country['country'].str.replace(r"\([^()]*\)", "").str.strip()

#%%
# Grouping GDELT by country to merge

gdelt['MonthYear'] = gdelt.apply(lambda row: str(row['year']) + str(row['month']).zfill(2), axis=1)

count_events_cols = gdelt.filter(like='count_events').columns.tolist()

gdelt_country = gdelt.groupby(['MonthYear', 'isocode', 'month', 'year'])[count_events_cols].sum().reset_index()

gdelt_country2 = gdelt.groupby(['MonthYear', 'isocode', 'month', 'year']).agg(
    Actor1Code_avgcount = ('Actor1Code_count', 'mean'),
    Actor1CountryCode_avgcount = ('Actor1CountryCode_count','mean'),
    Actor1KnownGroupCode_avgcount = ('Actor1KnownGroupCode_count','mean'),
    Actor1Type1Code_avgcount = ('Actor1Type1Code_count','mean'),
    Actor2Code_avgcount = ('Actor2Code_count','mean'),
    Actor2CountryCode_avgcount = ('Actor2CountryCode_count', 'mean'), 
    Actor2KnownGroupCode_avgcount = ('Actor2KnownGroupCode_count', 'mean'),
    Actor2Type1Code_avgcount = ('Actor2Type1Code_count', 'mean'),
    AvgGoldsteinScale = ('AvgGoldsteinScale', 'mean')).reset_index()

gdelt_country2.head(10)

#%%
# Putting the gdelt groupbys together

gdelt_country = pd.merge(gdelt_country, gdelt_country2, on=['MonthYear', 'isocode', 'month', 'year'], how='inner')

gdelt_country.head(10)
#%%
# Using pycountry to get ISO codes from the name

def get_country_name(country_name):
    try:
        country = pycountry.countries.search_fuzzy(country_name)[0]
        return country.alpha_3
    except LookupError:
        return None
    
# Apply standardization to country names
ucdp_country['isocode'] = ucdp_country['country'].apply(get_country_name)

#%%
# Manually filling in the missing countries

print(ucdp_country[ucdp_country['isocode'].isnull()]['country'].unique())

missing_countries = ucdp_country[ucdp_country['isocode'].isnull()]

missing_countries.loc[missing_countries['country'] == 'DR Congo', 'isocode'] = pycountry.countries.search_fuzzy('Congo')[1].alpha_3
missing_countries.loc[missing_countries['country'] == 'Laos', 'isocode'] = pycountry.countries.search_fuzzy('Lao')[0].alpha_3
missing_countries.loc[missing_countries['country'] == 'Ivory Coast', 'isocode'] = pycountry.countries.search_fuzzy('Cote')[0].alpha_3
missing_countries.loc[missing_countries['country'] == 'Macedonia, FYR', 'isocode'] = pycountry.countries.search_fuzzy('Macedonia')[0].alpha_3

# Update the original DataFrame with the filled-in values
ucdp_country.update(missing_countries)
ucdp_country.loc[ucdp_country['country'] == 'Niger', 'isocode'] = pycountry.countries.search_fuzzy('Niger')[1].alpha_3

#%%
# Merge UCDP and GDELT

ucdp_country['MonthYear'] = ucdp_country['MonthYear'].astype(int)
gdelt_country['MonthYear'] = gdelt_country['MonthYear'].astype(int)

full_df = pd.merge(gdelt_country, ucdp_country, on=['isocode', 'MonthYear', 'month', 'year'], how='left')

#%%

full_df.head(10)

full_df.to_csv(path + '/data/merged.csv', index=False)
