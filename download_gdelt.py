
import pandas as pd
import time
import requests
from requests import RequestException # all possible errors when downloading centered in one exception
# from requests import ReadTimeout, ConnectTimeout, HTTPError, Timeout, ConnectionError # possible errors when downloading
from bs4 import BeautifulSoup
from download import download
import os
import numpy as np
from urllib.error import HTTPError

'''

Author: Bruno Conte | bruno.conte@barcelonagse.eu

In this current version, I skip the gdelt package and directly download the GDELT
events' csv from the raw events' database of GDELT here:

http://data.gdeltproject.org/events/index.html

'''

# Step 1: put here the directory of the GDELTnowcast on Dropbox:
dfolder = "data/"
os.makedirs(dfolder, exist_ok=True)

# Step 2: # GDELT (header) names; I manually input it in every
# iteration. Important, there are two types of headers: before and after March2013.
cnames_bef = ['GLOBALEVENTID', 'SQLDATE', 'MonthYear', 'Year', 'FractionDate', 'Actor1Code', 'Actor1Name', 'Actor1CountryCode', 'Actor1KnownGroupCode', 'Actor1EthnicCode', 'Actor1Religion1Code', 'Actor1Religion2Code', 'Actor1Type1Code', 'Actor1Type2Code', 'Actor1Type3Code', 'Actor2Code', 'Actor2Name', 'Actor2CountryCode', 'Actor2KnownGroupCode', 'Actor2EthnicCode', 'Actor2Religion1Code', 'Actor2Religion2Code', 'Actor2Type1Code', 'Actor2Type2Code', 'Actor2Type3Code', 'IsRootEvent', 'EventCode', 'EventBaseCode', 'EventRootCode', 'QuadClass', 'GoldsteinScale', 'NumMentions', 'NumSources', 'NumArticles', 'AvgTone', 'Actor1Geo_Type', 'Actor1Geo_FullName', 'Actor1Geo_CountryCode', 'Actor1Geo_ADM1Code', 'Actor1Geo_Lat', 'Actor1Geo_Long', 'Actor1Geo_FeatureID', 'Actor2Geo_Type', 'Actor2Geo_FullName', 'Actor2Geo_CountryCode', 'Actor2Geo_ADM1Code', 'Actor2Geo_Lat', 'Actor2Geo_Long', 'Actor2Geo_FeatureID', 'ActionGeo_Type', 'ActionGeo_FullName', 'ActionGeo_CountryCode', 'ActionGeo_ADM1Code', 'ActionGeo_Lat', 'ActionGeo_Long', 'ActionGeo_FeatureID', 'DATEADDED']
cnames_aft = ['GLOBALEVENTID', 'SQLDATE', 'MonthYear', 'Year', 'FractionDate', 'Actor1Code', 'Actor1Name', 'Actor1CountryCode', 'Actor1KnownGroupCode', 'Actor1EthnicCode', 'Actor1Religion1Code', 'Actor1Religion2Code', 'Actor1Type1Code', 'Actor1Type2Code', 'Actor1Type3Code', 'Actor2Code', 'Actor2Name', 'Actor2CountryCode', 'Actor2KnownGroupCode', 'Actor2EthnicCode', 'Actor2Religion1Code', 'Actor2Religion2Code', 'Actor2Type1Code', 'Actor2Type2Code', 'Actor2Type3Code', 'IsRootEvent', 'EventCode', 'EventBaseCode', 'EventRootCode', 'QuadClass', 'GoldsteinScale', 'NumMentions', 'NumSources', 'NumArticles', 'AvgTone', 'Actor1Geo_Type', 'Actor1Geo_FullName', 'Actor1Geo_CountryCode', 'Actor1Geo_ADM1Code', 'Actor1Geo_Lat', 'Actor1Geo_Long', 'Actor1Geo_FeatureID', 'Actor2Geo_Type', 'Actor2Geo_FullName', 'Actor2Geo_CountryCode', 'Actor2Geo_ADM1Code', 'Actor2Geo_Lat', 'Actor2Geo_Long', 'Actor2Geo_FeatureID', 'ActionGeo_Type', 'ActionGeo_FullName', 'ActionGeo_CountryCode', 'ActionGeo_ADM1Code', 'ActionGeo_Lat', 'ActionGeo_Long', 'ActionGeo_FeatureID', 'DATEADDED', 'SOURCEURL']

# Step 3: list of variables to be created in the final dataset:
var_list = ['count_events_1', 'count_events_2', 'count_events_3', 'count_events_4', 'count_events_5', 'count_events_6', 'count_events_7', 'count_events_8',
'count_events_9', 'count_events_10', 'count_events_11', 'count_events_12', 'count_events_13', 'count_events_14', 'count_events_15', 'count_events_16', 'count_events_17', 'count_events_18', 'count_events_19', 'count_events_20',
'count_events_1_gov', 'count_events_2_gov', 'count_events_3_gov', 'count_events_4_gov', 'count_events_5_gov', 'count_events_6_gov', 'count_events_7_gov',
'count_events_8_gov', 'count_events_9_gov', 'count_events_10_gov', 'count_events_11_gov', 'count_events_12_gov', 'count_events_13_gov', 'count_events_14_gov',
'count_events_15_gov', 'count_events_16_gov', 'count_events_17_gov', 'count_events_18_gov', 'count_events_19_gov', 'count_events_20_gov', 'count_events_1_opp', 'count_events_2_opp', 'count_events_3_opp', 'count_events_4_opp', 'count_events_5_opp', 'count_events_6_opp', 'count_events_7_opp',
'count_events_8_opp', 'count_events_9_opp', 'count_events_10_opp', 'count_events_11_opp', 'count_events_12_opp', 'count_events_13_opp', 'count_events_14_opp',
'count_events_15_opp', 'count_events_16_opp', 'count_events_17_opp', 'count_events_18_opp', 'count_events_19_opp', 'count_events_20_opp']

# Step 4: creating list of links of all GDELT events' files:
links = requests.get('http://data.gdeltproject.org/events/index.html')
links = BeautifulSoup(links.content, "html5lib") # processing its content
links = links.find_all('a') # getting the links

# Step 5: creating (or loading) the list of files that have been already
# downloaded + those that we do not want. This list is updated every 100
# iterations of the loop and stored as "gdelt_downloaded_files.txt". It
# also contains the number of the last counter to avoid the files to overwrite.

try: # if the list was previously saved we load it.
    dfiles = np.genfromtxt(dfolder + 'gdelt_downloaded_files.txt', delimiter = '\t', dtype="str").tolist()
    # the number of the counter where the previous iterations stopped; set this
    # only if you area downloading the whole database
    # ii = int(dfiles[-1])+1
    # otherwise (for monthly updates), set it to zero:
    ii = 0
    dfiles = dfiles[:-1]
except: # if the files does not exist (start from scratch)
    dfiles = ['md5sums', 'filesizes', 'GDELT.MASTERREDUCEDV2.1979-2013.zip']
    ii = 0

# Step 4: looping over list of links obtained above:
df_gdelt = pd.DataFrame()

for l in links:
    if l['href'] not in dfiles:

        print('****************\n\nRetrieving ' + l['href'] + " (" + str(ii + 1) + "/" + str(len(links) - 3) + ')...\n')
        durl = 'http://data.gdeltproject.org/events/' + l['href']

        try:
            try:
                path = download(durl, dfolder, kind="zip", replace=True)
            except (RequestException, HTTPError, RuntimeError) as e:
                if isinstance(e, HTTPError) and e.code == 404:
                    print("Failed to download " + l['href'] + " due to 404 error. Skipping...")
                    continue
                else:
                    raise e
            down = True
            dfiles.append(l['href'])
            fname = os.listdir(dfolder)
            fname = [i for i in fname if i.lower().endswith('.csv')]
            fname = fname[0]

            if int(fname[0:4]) < 2013:
                results = pd.read_csv(dfolder + fname, sep="\t", header=None, names=cnames_bef, low_memory=False)
            elif int(fname[0:4]) == 2013 & int(fname[4:6]) <= 3:
                results = pd.read_csv(dfolder + fname, sep="\t", header=None, names=cnames_bef, low_memory=False)
            else:
                results = pd.read_csv(dfolder + fname, sep="\t", header=None, names=cnames_aft, low_memory=False)

        except (RequestException, HTTPError, RuntimeError) as e:
            down = False
            if isinstance(e, HTTPError) and e.code == 404:
                print("Failed to download " + l['href'] + " due to 404 error. Skipping...")
                continue
            time.sleep(30)

        # if the file was downloaded:
        if down==True:
            # loading the .csv file:
            fname = os.listdir(dfolder) # getting the file name
            fname = [i for i in fname if i.lower().endswith('.csv')] # remove system files from the list
            fname = fname[0]
            # importing as a pandas dataframe. Importantly, I need to check which header to use
            # as it changes after march-2013:
            if int(fname[0:4])<2013: # before 2013
                results = pd.read_csv(dfolder + fname, sep = "\t", header = None, names = cnames_bef,low_memory=False)
            elif (int(fname[0:4])==2013 & int(fname[4:6])<=3): # jan to march 2013
                results = pd.read_csv(dfolder + fname, sep = "\t", header = None, names = cnames_bef,low_memory=False)
            else: # after march 2013
                results = pd.read_csv(dfolder + fname, sep = "\t", header = None, names = cnames_aft,low_memory=False)

            # counting all events found by country/event type:
            df1 = results.groupby(['MonthYear', 'ActionGeo_CountryCode', 'EventRootCode']).size().reset_index(name = 'counts')
            # sometimes the events have no rootcode so that I cannot make it an
            # integer. I remove those:
            df1['EventRootCode'] = pd.to_numeric(df1['EventRootCode'],errors='coerce') # forces event codes to numeric; those non numeric became NaN
            df1 = df1.dropna(subset=['EventRootCode']) # removing these
            df1['EventRootCode'] = df1['EventRootCode'].astype(int)
            # reshaping it
            df1 = df1.pivot_table(values = 'counts', index = ['ActionGeo_CountryCode', 'MonthYear'], columns = 'EventRootCode',aggfunc=np.sum).add_prefix('count_events_')
            df1 = df1.reset_index()
            # df1 = df1.pivot(index=['ActionGeo_CountryCode', 'MonthYear'], columns = 'EventRootCode', values = 'counts').reset_index()

            # the same but for the events in which actor1 or 2 is the government:
            df2 = results.loc[(results['Actor1Type1Code']=='GOV') | (results['Actor2Type1Code']=='GOV') | (results['Actor1Type1Code']=='COP') | (results['Actor2Type1Code']=='COP') | (results['Actor1Type1Code']=='MIL') | (results['Actor2Type1Code']=='MIL')]
            # df2 = results.loc[(results['Actor1Type1Code']=='GOV') | (results['Actor2Type1Code']=='GOV')]
            df2 = df2.groupby(['MonthYear', 'ActionGeo_CountryCode', 'EventRootCode']).size().reset_index(name = 'counts')
            df2['EventRootCode'] = pd.to_numeric(df2['EventRootCode'],errors='coerce') # forces event codes to numeric; those non numeric became NaN
            df2 = df2.dropna(subset=['EventRootCode']) # removing these
            df2['EventRootCode'] = df2['EventRootCode'].astype(int)
            df2 = df2.pivot_table(values = 'counts', index = ['ActionGeo_CountryCode', 'MonthYear'], columns = 'EventRootCode',aggfunc=np.sum).add_prefix('count_events_').add_suffix('_gov')
            # df2 = df2.pivot(index='ActionGeo_CountryCode', columns = 'EventRootCode', values = 'counts').add_suffix('_gov')
            df2 = df2.reset_index()

            # the same but for the events in which actor1 is the opposition:
            df3 = results.loc[(results['Actor1Type1Code']=='INS') | (results['Actor1Type1Code']=='OPP') | (results['Actor1Type1Code']=='REB') | (results['Actor2Type1Code']=='SEP')]
            df3 = df3.groupby(['MonthYear', 'ActionGeo_CountryCode', 'EventRootCode']).size().reset_index(name = 'counts')
            df3['EventRootCode'] = pd.to_numeric(df3['EventRootCode'],errors='coerce') # forces event codes to numeric; those non numeric became NaN
            df3 = df3.dropna(subset=['EventRootCode']) # removing these
            df3['EventRootCode'] = df3['EventRootCode'].astype(int)
            df3 = df3.pivot_table(values = 'counts', index = ['ActionGeo_CountryCode', 'MonthYear'], columns = 'EventRootCode',aggfunc=np.sum).add_prefix('count_events_').add_suffix('_opp')
            df3 = df3.reset_index()

            # merging them:
            df = pd.merge(df1,df2,how='outer')
            df = pd.merge(df,df3,how='outer')
            # adding year and month on Masterfile's format:
            df['year'] = [int(str(i)[0:4]) for i in df['MonthYear'].tolist()]
            df['month'] = [int(str(i)[4:6]) for i in df['MonthYear'].tolist()]
            # removing monthyear var:
            df = df.drop(columns=['MonthYear'])

            # make sure the dataset has all variable names (assign zero misses some):
            for cname in var_list:
                if cname not in df.columns:
                    df[cname]=0

            if ii in np.arange(0,100001,100): # the initial or every hundredth iteration
                df_gdelt = df
            else: # appending it:
                df_gdelt = pd.concat([df_gdelt, df], ignore_index=True)

                # here I aggregate (collapse) again the data by month-year-country
                # to make sure that events dated in time periods different than the
                # file are aggregated in their corrected date group. Importantly, events
                # do not repeat across different bulk files.
                df_gdelt = df_gdelt.groupby(['ActionGeo_CountryCode', 'year', 'month'])['count_events_1', 'count_events_2', 'count_events_3', 'count_events_4', 'count_events_5', 'count_events_6', 'count_events_7', 'count_events_8',
                'count_events_9', 'count_events_10', 'count_events_11', 'count_events_12', 'count_events_13', 'count_events_14', 'count_events_15', 'count_events_16', 'count_events_17', 'count_events_18', 'count_events_19', 'count_events_20',
                'count_events_1_gov', 'count_events_2_gov', 'count_events_3_gov', 'count_events_4_gov', 'count_events_5_gov', 'count_events_6_gov', 'count_events_7_gov',
                'count_events_8_gov', 'count_events_9_gov', 'count_events_10_gov', 'count_events_11_gov', 'count_events_12_gov', 'count_events_13_gov', 'count_events_14_gov',
                'count_events_15_gov', 'count_events_16_gov', 'count_events_17_gov', 'count_events_18_gov', 'count_events_19_gov', 'count_events_20_gov', 'count_events_1_opp', 'count_events_2_opp', 'count_events_3_opp', 'count_events_4_opp', 'count_events_5_opp', 'count_events_6_opp', 'count_events_7_opp',
                'count_events_8_opp', 'count_events_9_opp', 'count_events_10_opp', 'count_events_11_opp', 'count_events_12_opp', 'count_events_13_opp', 'count_events_14_opp',
                'count_events_15_opp', 'count_events_16_opp', 'count_events_17_opp', 'count_events_18_opp', 'count_events_19_opp', 'count_events_20_opp'].sum().reset_index()

            del df, df1, df2, df3, results # cleaning RAM
            os.remove(dfolder + fname) # deleting temp .csv file
            time.sleep(5)

        if ii in np.arange(99,100000,100):
            # exporting the temp 100th file:
            df_gdelt.to_csv(dfolder + 'gdelt_reshaped_' + str(ii) + '.txt', index=False)
            # exporting the list of files ever downloaded and the number of the last iteration
            dfiles.append(str(ii))
            np.savetxt(dfolder + 'gdelt_downloaded_files.txt', dfiles, delimiter="\t", fmt="%s")
            dfiles = dfiles[:-1]
        ii+=1

# saving the files for the last iteration:
df_gdelt.to_csv(dfolder + 'gdelt_reshaped_' + str(ii) + '.txt', index=False)
dfiles.append(str(ii))
np.savetxt(dfolder + 'gdelt_downloaded_files.txt', dfiles, delimiter="\t", fmt="%s")

# Step 7: load all temp txt of the 100 iteration blocks and
# append them together. As bulk files can contain events of other periods,
# I collapse the data by country-year-month once again just to be sure.

# list of files:
files = os.listdir(dfolder)
files = [i for i in files if i.lower().startswith('gdelt_reshaped_')]

# looping over:
print("\n****************\n\nAppending files to a unique dataset...\n")
cc = 0 # counter
for f in files:
    df = pd.read_csv(dfolder + f)
    if cc==0:
        df_gdelt = df
    else:
        df_gdelt = df_gdelt.append(df,ignore_index=True)
    # final collapse:
    df_gdelt = df_gdelt.groupby(['ActionGeo_CountryCode', 'year', 'month'])['count_events_1', 'count_events_2', 'count_events_3', 'count_events_4', 'count_events_5', 'count_events_6', 'count_events_7', 'count_events_8',
    'count_events_9', 'count_events_10', 'count_events_11', 'count_events_12', 'count_events_13', 'count_events_14', 'count_events_15', 'count_events_16', 'count_events_17', 'count_events_18', 'count_events_19', 'count_events_20',
    'count_events_1_gov', 'count_events_2_gov', 'count_events_3_gov', 'count_events_4_gov', 'count_events_5_gov', 'count_events_6_gov', 'count_events_7_gov',
    'count_events_8_gov', 'count_events_9_gov', 'count_events_10_gov', 'count_events_11_gov', 'count_events_12_gov', 'count_events_13_gov', 'count_events_14_gov',
    'count_events_15_gov', 'count_events_16_gov', 'count_events_17_gov', 'count_events_18_gov', 'count_events_19_gov', 'count_events_20_gov', 'count_events_1_opp', 'count_events_2_opp', 'count_events_3_opp', 'count_events_4_opp', 'count_events_5_opp', 'count_events_6_opp', 'count_events_7_opp',
    'count_events_8_opp', 'count_events_9_opp', 'count_events_10_opp', 'count_events_11_opp', 'count_events_12_opp', 'count_events_13_opp', 'count_events_14_opp',
    'count_events_15_opp', 'count_events_16_opp', 'count_events_17_opp', 'count_events_18_opp', 'count_events_19_opp', 'count_events_20_opp'].sum().reset_index()
    cc+=1
    del df # cleaning RAM

# Step 8: adding correct isocodes (GDELT uses the FIPS-2-digit isocode) to
# match the masterfile:

print("\n****************\n\nAdding correct ISO codes...\n")
ccodes = pd.read_csv(dfolder + 'country_codes_fips_to_iso3c.txt', sep='\t') # codes' list
# merging them:
df_gdelt = pd.merge(df_gdelt,ccodes,how='inner', left_on = 'ActionGeo_CountryCode', right_on = 'actiongeo_countrycode')
# removing useless variables:
df_gdelt =df_gdelt.drop(columns=['actiongeo_countrycode', 'ActionGeo_CountryCode'])

# Step 9: removing the temp files with 100 iterations each:
#files = [os.remove(dfolder + fname) for fname in files]

# Step 10: Appending to the previously downloaded and processed data; saving
# otherwise:
print("\n****************\n\nDone! :) Exporting final dataset...\n")
if 'final_gdelt_bycountry.txt' in os.listdir(dfolder):
    # loading and appending
    df_gdelt_2 = pd.read_csv(dfolder + 'final_gdelt_bycountry.txt') # previously downloaded data
    df_gdelt = df_gdelt_2.append(df_gdelt,ignore_index=True)
    # final collapse:
    df_gdelt = df_gdelt.groupby(['isocode', 'year', 'month'])['count_events_1', 'count_events_2', 'count_events_3', 'count_events_4', 'count_events_5', 'count_events_6', 'count_events_7', 'count_events_8',
    'count_events_9', 'count_events_10', 'count_events_11', 'count_events_12', 'count_events_13', 'count_events_14', 'count_events_15', 'count_events_16', 'count_events_17', 'count_events_18', 'count_events_19', 'count_events_20',
    'count_events_1_gov', 'count_events_2_gov', 'count_events_3_gov', 'count_events_4_gov', 'count_events_5_gov', 'count_events_6_gov', 'count_events_7_gov',
    'count_events_8_gov', 'count_events_9_gov', 'count_events_10_gov', 'count_events_11_gov', 'count_events_12_gov', 'count_events_13_gov', 'count_events_14_gov',
    'count_events_15_gov', 'count_events_16_gov', 'count_events_17_gov', 'count_events_18_gov', 'count_events_19_gov', 'count_events_20_gov', 'count_events_1_opp', 'count_events_2_opp', 'count_events_3_opp', 'count_events_4_opp', 'count_events_5_opp', 'count_events_6_opp', 'count_events_7_opp',
    'count_events_8_opp', 'count_events_9_opp', 'count_events_10_opp', 'count_events_11_opp', 'count_events_12_opp', 'count_events_13_opp', 'count_events_14_opp',
    'count_events_15_opp', 'count_events_16_opp', 'count_events_17_opp', 'count_events_18_opp', 'count_events_19_opp', 'count_events_20_opp'].sum().reset_index()
    del df_gdelt_2
    # saving it:
    df_gdelt.to_csv(dfolder + 'final_gdelt_bycountry.txt', index=False) # .csv
else:
    df_gdelt.to_csv(dfolder + 'final_gdelt_bycountry.txt', index=False) # .csv