import pandas as pd
from generic import functions, paths
import os
import numpy as np


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

    # Convert createdAt to datetime and remove rows where it can't be parsed
    df['createdAt'] = pd.to_datetime(df['createdAt'], errors='coerce')  # coerce non-dates into NaT
    df = df.dropna(subset=['createdAt'])

    # Coerce missing countryCode to 'Unknown'
    df['countryCode'] = df['countryCode'].replace('', 'Unknown').fillna('Unknown')
    df['platform'] = df['platform'].replace('', 'Unknown').fillna('Unknown')

    # Extract date from createdAt
    df['date'] = df['createdAt'].dt.date

    # Group by userId, date, countryCode, and platform and sum the amount
    user_revenue = df.groupby(['userId', 'date', 'countryCode', 'platform'])['amount'].sum().reset_index()

    # Rename the amount column to total_revenue
    user_revenue.rename(columns={'amount': 'total_revenue'}, inplace=True)

    return user_revenue


dataframes = get_files_in_dictionary()
user_daily_revenue = aggregate_revenue_at_user_level()
installs = dataframes['installs']
installs['installedAt'] = pd.to_datetime(installs['installedAt']).dt.date


revenue_x_installs = pd.merge(left=user_daily_revenue, right=installs, how='outer', on='userId')
revenue_x_installs['countryCode_x'].fillna('Unknown', inplace=True)
revenue_x_installs['index'] = revenue_x_installs.apply(functions.create_index, axis=1)
grouped_revenues = revenue_x_installs.groupby(['installedAt', 'channel', 'index']).agg(
    installs=('userId', 'count'),
    installs_revenue=('total_revenue', lambda x: (x > 0).sum()),
    total_revenue=('total_revenue', 'sum')
).reset_index()


adspend = dataframes['adspend']
adspend['index'] = adspend.apply(functions.create_index, axis=1)
adspend['installs_adjusted'] = np.maximum(adspend['network_installs'], adspend['installs'])

grouped_revenues.rename(columns={'countryCode_x': 'country_code'}, inplace=True)
adspend_x_revenue = pd.merge(grouped_revenues, adspend,  how='left', on=['index'])


total_revenue = grouped_revenues['total_revenue'].sum()