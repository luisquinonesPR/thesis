def make_target_regr(df: pd.DataFrame, shifters: dict, target: str):
    """
    Generate shifted variables and calculate the maximum for multiple shifters.

    Args:
        df (pd.DataFrame): The input DataFrame.
        shifters (dict): A dictionary specifying the shifters.
            Example: {3: 'w3_', 6: 'w6_'} for 2 shifters with prefixes 'w3_' and 'w6_'.
        target (str): The column name of the target variable.

    Returns:
        pd.DataFrame: The modified DataFrame with the maximum for each shifter.

    """
    for shifter, prefix in shifters.items():
        # Loop through each period and generate the shift variables
        for i in range(1, shifter + 1):
            col_name = f'{prefix}{target}{i}'
            df[col_name] = df.groupby('isocode')[target].shift(-i)

        # Take the maximum for t periods forward and create the new variable
        avg_col_name = f'{prefix}target_regr'
        df[avg_col_name] = df[[f'{prefix}{target}{i}' for i in range(1, shifter + 1)]].mean(axis=1, skipna=False)

        # Drop the shift variables
        df = df.drop(columns=[f'{prefix}{target}{i}' for i in range(1, shifter + 1)])

        # Shift the resulting column by 1 so that Luis can shift back in LSTM
        df[avg_col_name] = df[avg_col_name].shift(1)

    return df


def make_target_clsf(df: pd.DataFrame, shifters: dict, target: str):
    """
    Generate shifted variables and calculate the maximum for multiple shifters.

    Args:
        df (pd.DataFrame): The input DataFrame.
        shifters (dict): A dictionary specifying the shifters.
            Example: {3: 'w3_', 6: 'w6_'} for 2 shifters with prefixes 'w3_' and 'w6_'.
        target (str): The column name of the target variable.

    Returns:
        pd.DataFrame: The modified DataFrame with the maximum for each shifter.

    """
    for shifter, prefix in shifters.items():
        # Loop through each period and generate the shift variables
        for i in range(1, shifter + 1):
            col_name = f'{prefix}{target}{i}'
            df[col_name] = df.groupby('isocode')[target].shift(-i)

        # Take the maximum for t periods forward and create the new variable
        max_col_name = f'{prefix}target_clsf'
        df[max_col_name] = df[[f'{prefix}{target}{i}' for i in range(1, shifter + 1)]].max(axis=1, skipna=False)

        # Drop the shift variables
        df = df.drop(columns=[f'{prefix}{target}{i}' for i in range(1, shifter + 1)])

        # Shift the resulting column by 1 so that Luis can shift back in LSTM
        df[max_col_name] = df[max_col_name].shift(1)

    return df

shifters = {3: 'w3_', 6: 'w6_'}
df = make_target_clsf(df, shifters, 'clsf')
df = make_target_regr(df, shifters, 'regr')