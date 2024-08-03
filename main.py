import pandas as pd
import os
import numpy as np
from generic import functions, paths

def get_files_in_dictionary(names, base_path):
    """Extracts files from path and automatically names them based on the prefix to .csv specified in names as key
    elements within the dataframes dictionary"""
    filepaths = [os.path.join(base_path, name) for name in names]
    dataframes = {os.path.splitext(os.path.basename(filepath))[0]: pd.read_csv(filepath) for filepath in filepaths}
    return dataframes


def process_revenue_dataframe(df):
    """Processes the revenue dataframe to clean data and aggregate revenue at user level."""
    df['createdAt'] = pd.to_datetime(df['createdAt'], errors='coerce')
    df = df.dropna(subset=['createdAt'])
    df['countryCode'] = df['countryCode'].replace('', 'Unknown').fillna('Unknown')
    df['platform'] = df['platform'].replace('', 'Unknown').fillna('Unknown')
    df['date'] = df['createdAt'].dt.date
    user_revenue = df.groupby(['userId', 'date', 'countryCode', 'platform'])['amount'].sum().reset_index()
    user_revenue.rename(columns={'amount': 'total_revenue'}, inplace=True)
    return user_revenue


def prepare_installs_dataframe(df):
    """Prepares the installs dataframe by converting dates."""
    df['installedAt'] = pd.to_datetime(df['installedAt']).dt.date
    return df


def merge_revenue_installs(user_daily_revenue, installs):
    """Merges the user daily revenue and installs dataframes."""
    merged_df = pd.merge(left=user_daily_revenue, right=installs, how='outer', on='userId')
    merged_df['countryCode_x'].fillna('Unknown', inplace=True)
    return merged_df


def create_index_and_group_revenues(merged_df, create_index_func):
    """Creates index and groups revenues."""
    merged_df['index'] = merged_df.apply(create_index_func, axis=1)
    grouped_revenues = merged_df.groupby(['installedAt', 'channel', 'index']).agg(
        installs=('userId', 'count'),
        installs_revenue=('total_revenue', lambda x: (x > 0).sum()),
        total_revenue=('total_revenue', 'sum')
    ).reset_index()
    grouped_revenues.rename(columns={'countryCode_x': 'country_code'}, inplace=True)
    return grouped_revenues


def process_adspend_dataframe(adspend, create_index_func):
    """Processes the adspend dataframe."""
    adspend['index'] = adspend.apply(create_index_func, axis=1)
    adspend['installs_adjusted'] = np.maximum(adspend['network_installs'], adspend['installs'])
    return adspend


def merge_adspend_revenue(grouped_revenues, adspend):
    """Merges grouped revenues with adspend data."""
    return pd.merge(grouped_revenues, adspend, how='outer', on=['index'])



base_path = paths.SUBDIRFILES
filenames = ['adspend.csv', 'installs.csv', 'revenue.csv']

dataframes = get_files_in_dictionary(filenames, base_path)

user_daily_revenue = process_revenue_dataframe(dataframes['revenue'])
installs = prepare_installs_dataframe(dataframes['installs'])
merged_revenue_installs = merge_revenue_installs(user_daily_revenue, installs)

grouped_revenues = create_index_and_group_revenues(merged_revenue_installs, functions.create_index)
adspend = process_adspend_dataframe(dataframes['adspend'], functions.create_index)

adspend_x_revenue = merge_adspend_revenue(grouped_revenues, adspend)
total_revenue = adspend_x_revenue['total_revenue'].sum()
total_cost = adspend_x_revenue['cost'].sum()

