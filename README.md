# Predicting Conflict Escalation Using GDELT Events Database

**Research Team:**

- Giovanna Chaves
- Margherita Philipp
- Luis Quiñones 

## Repository Structure

- In the `utils` folder, you can find all the functions used for preprocessing stages.
- In the `models` folder, you can find the machine learning models used in the research, including the Random Forest Classifier and Long Short-Term Memory (LSTM) network for predicting binary conflict based on 50 deaths per 100,000 individuals.
- In the`eda` folder, you can find all the notebooks and scripts associated with the exploratory data analysis of the combined UCDP and GDELT data.
- In the `data_acquisition` folder, you will find the scripts used to download and aggregate GDELT and UCDP data.


## Notebooks

The research process is documented in detail through a series of Jupyter notebooks:

1. `data_acquisition`: Preprocessing of the UCDP data, including cleaning and subsetting for the selected countries and the process of downloading and aggregating GDELT event data.
3. `eda`: Exploratory data analysis of the combined UCDP and GDELT data.
4. `models`: Training and evaluation of the Random Forest classifier, and regressor model for conflict prediction as well as training and evaluation of the LSTM classifier and regressor for conflict prediction.

## Data 

The research uses monthly data from the year 2000 onwards. The final dataset contains information from UCDP and GDELT, including:

- `isocode`: isocode of the country.
- `year`: The year of the data entry.
- `month`: The year of the data entry.
- `deaths`: Total number of battle-related deaths as estimated by UCDP.
- `share_event_counts`: Share of GDELT events associated with a given country at time t. 
- `armedconf`: Binary classification 50 deaths per 100,000. 
- `escalation`: Percentage change of historical patterns in deaths. 
- GDELT features: Various aggregated GDELT event data features.

The conflict is predicted based on a binary classification where 50 deaths per 100,000 individuals signifies a conflict. Escalation is defined as months in which the number of deaths increases significantly compared to historical patterns, surpassing the threshold of 0.05 deaths and exceeding the 75th percentile of the previous 24 months' percentage change in deaths. The models predict this classification across various countries using the above-mentioned features. 

We also predict a regression target where we set a `deaths_all_pc` target, where we predict the number of deaths per capita.
Our predictions are made for three time horizons: one month ahead, three months ahead, and six months ahead.

The data used to develop and validate our models is available through the link below:

[Google Drive Data for Project](https://drive.google.com/drive/folders/1zlgJUf_E1Lf6OsTS-gEvF1hvO4WfKWgs?usp=drive_link)

## Disclaimer

Due to the inherent stochasticity in the models, results may not be perfectly reproducible. Please consider this when running and interpreting the models.
