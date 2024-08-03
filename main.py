import pandas as pd
from generic import functions, paths
import os


def get_files_in_dictionary():
    """Extracts files from path and automatically names them based on the prefix to .csv specified in names as key
    elements within the dataframes dictionary"""

    names = ['adspend.csv', 'installs.csv', 'revenue.csv']
    filepaths = functions.join_filenames(paths.SUBDIRFILES, names)
    dataframes = {}
    for filepath in filepaths:
        key = os.path.splitext(os.path.basename(filepath))[0]
        dataframes[key] = pd.read_csv(filepath)
    return dataframes


def aggregate_revenue_at_user_level():
    """
    This function takes the dataframe revenues, adjusts the createdAt to be a date
    (and removes the one row with nonsense data in which the string cannot be parsed into a date),
    then calculates the revenue per day per user keeping also the country code and platform.
    This configuration was set up because it is the same aggregation level as the adspend table.
    :return:
    """
    df = dataframes['revenue']
    df['createdAt'] = pd.to_datetime(df['createdAt'], errors='coerce') # coerce non-dates into NaT
    df = df.dropna(subset=['createdAt'])
    df['date'] = df['createdAt'].dt.date
    user_revenue = df.groupby(['userId', 'date', 'countryCode', 'platform'])['amount'].sum().reset_index()
    user_revenue.rename(columns={'amount': 'total_revenue'}, inplace=True)
    return user_revenue


dataframes = get_files_in_dictionary()
user_daily_revenue = aggregate_revenue_at_user_level()