# loading libraries 

import matplotlib as plt
import pandas as pd
import os
import pycountry
import string
from unidecode import unidecode
import category_encoders as ce

path = os.getcwd()

# loading the data

full = pd.read_csv(path + '/data/merged.csv')

# Dropping columns before 1989
full.drop(full.loc[full['year']<1989].index, inplace=True)

# Filling missing UCDP data with 0
full[["deaths","state_deaths","nonstate_deaths","onesided_deaths","civilian_deaths","avg_sources","conflict_counts",
     "conflict_freq","dyad_counts","dyad_freq"]] = full[["deaths","state_deaths","nonstate_deaths","onesided_deaths",
                                                         "civilian_deaths","avg_sources","conflict_counts",
                                                         "conflict_freq","dyad_counts","dyad_freq"]].fillna(0)

