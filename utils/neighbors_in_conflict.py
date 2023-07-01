df = pd.read_csv(os.path.abspath(parent_path + '/data/preprocessed_df.csv'))
merged_df = df.merge(pd.DataFrame(isocode_dict.items(), columns=['isocode', 'neighbors']), on='isocode', how='left')

def check_neighbor_conflict(row, df):
    """
    Check if at least one neighbor is in conflict based on the row of the dataframe.

    Args:
        row (pd.Series): Row of the dataframe containing country information.
        df (pd.DataFrame): The entire dataframe containing country and neighbor information.

    Returns:
        bool: True if at least one neighbor is in conflict, False otherwise.
    """
    neighbors = row['neighbors']
    if isinstance(neighbors, list):
        neighbor_status = merged_df[
            (merged_df['isocode'].isin(neighbors)) &
            (merged_df['month_year'] == row['month_year'])
        ]['armedconf_intp_pop']
        if len(neighbor_status) > 0 and neighbor_status.any():
            return True
    return False

# Apply the function row-wise to create the new column
merged_df['neighbor_conflict'] = merged_df.apply(check_neighbor_conflict, axis=1, df=merged_df)
