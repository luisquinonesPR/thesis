def interpolate_group(group):
    group[columns_to_interpolate] = group[columns_to_interpolate].interpolate(method='linear')
    return group

def add_refugee_flows(preprocessed_df):
    """
    Add refugee flows within each country group and interpolate by month.

    Args:
        preprocessed_df (pandas.DataFrame): DataFrame containing the preprocessed data.

    Returns:
        pandas.DataFrame: DataFrame with interpolated refugee flows.

    """
    
    # Read UNHCR data
    df = pd.read_csv(os.path.abspath(parent_path + '/data/UNHCR.csv'))

    # Create 'Total' column
    df['Total'] = df.iloc[:, 5:9].sum(axis=1)

    # Retrieving the list of countries of origin and destination 
    common_countries_origin = set(df['Country of origin (ISO)']).intersection(set(preprocessed_df['isocode']))
    common_countries_destination = set(df['Country of asylum (ISO)']).intersection(set(preprocessed_df['isocode']))

    # Aggregating refugee flows at country of origin level
    df_origin_agg = df.groupby(['Country of origin (ISO)', 'Year'])['Total'].sum().reset_index()
    df_origin_agg.rename(columns={'Total': 'RefugeesAnnual'}, inplace=True)

    # Aggregating refugee flows at country of destination level
    df_destination_agg = df.groupby(['Country of asylum (ISO)', 'Year'])['Total'].sum().reset_index()
    df_destination_agg.rename(columns={'Total': 'RefugeesReceived'}, inplace=True)

    # Merge aggregated refugee flows with prepross
    df_wref = preprocessed_df.merge(df_origin_agg, left_on=['isocode', 'year'], right_on=['Country of origin (ISO)', 'Year'], how='left')
    df_wref = df_wref.merge(df_destination_agg, left_on=['isocode', 'year'], right_on=['Country of asylum (ISO)', 'Year'], how='left')
    df_wref.drop(['Country of origin (ISO)', 'Country of asylum (ISO)', 'Year_x', 'Year_y'], axis=1, inplace=True)

    # Fill NAs before 2023 with 0
    df_wref.loc[df_wref['year'] < 2023, ['RefugeesAnnual', 'RefugeesReceived']] = \
        df_wref.loc[df_wref['year'] < 2023, ['RefugeesAnnual', 'RefugeesReceived']].fillna(0)

    # Set values to NA for non-January months
    df_wref['month_year'] = pd.to_datetime(df_wref['month_year'], format='%Y-%m-%d')
    df_wref.loc[df_wref['month_year'].dt.month != 1, 'RefugeesAnnual'] = np.nan
    df_wref.loc[df_wref['month_year'].dt.month != 1, 'RefugeesReceived'] = np.nan

    # Set 'isocode' and 'month_year' as the index
    df_wref = df_wref.set_index(['isocode', 'month_year'])

    # List of columns to interpolate
    columns_to_interpolate = ['RefugeesAnnual', 'RefugeesReceived']

    # Apply interpolation within each country group
    df_wref = df_wref.groupby('isocode').apply(interpolate_group)

    # Reset the index
    df_wref = df_wref.reset_index(drop=False)

    return df_wref