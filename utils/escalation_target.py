def calculate_escalation(df):
    df['lag_deaths'] = df.groupby('isocode')['deaths'].shift(1)

    df['delta_deaths'] = np.where((df['lag_deaths'] == 0) & (df['deaths'] == 0), 0,
                                  np.where((df['lag_deaths'] == 0) & (df['deaths'] != 0), np.inf,
                                           np.where((df['lag_deaths']).isna() == True, 0,
                                                    (df['deaths'] - df['lag_deaths']) / df['lag_deaths'])))

    # Group the data by 'isocode' and calculate the 75th percentile of the previous 24 months' delta_deaths
    df['threshold'] = df.groupby('isocode')['delta_deaths'].transform(lambda x: x.shift(1).rolling(window=24, min_periods=1).quantile(0.75))
    df['threshold'] = df['threshold'].fillna(0)

    # Check if the current month's delta_deaths exceeds the threshold or is infinity
    df['escalation'] = (df['deaths'] >= 0.05) & ((df['delta_deaths'] > df['threshold']) | (df['delta_deaths'] == np.inf))
    df['escalation'] = df['escalation'].astype(int)

    return df

true_counts = df['escalation'].sum()
total_counts = df['escalation'].count()
percentage_true = (true_counts / total_counts) * 100

summary_table = pd.DataFrame({'True Count': pd.Series(true_counts), 'Percentage True': pd.Series(percentage_true)})
print(summary_table)