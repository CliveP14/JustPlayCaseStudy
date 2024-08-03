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


base_path = paths.SUBDIRFILES
filenames = ['adspend.csv', 'installs.csv', 'revenue.csv']
dataframes = get_files_in_dictionary(filenames, base_path)

revenue = dataframes['revenue']
revenue['createdAt'] = pd.to_datetime(revenue['createdAt'], errors='coerce')
revenue = revenue.dropna(subset=['createdAt'])

installs = dataframes['installs']
installs['index'] = installs.apply(functions.create_index, axis=1)

revenue_x_installs = pd.merge(revenue, installs, how='right', on='userId')
revenue_x_installs_grouped = revenue_x_installs.groupby(['index', 'channel', 'campaign', 'creative']).agg(
    installs=('userId', 'count'),
    users=('userId', 'nunique'),
    revenue=('amount', 'sum')
).reset_index()

adspend = dataframes['adspend']
adspend['max_value_installs'] = adspend[['network_installs', 'installs']].max(axis=1)
adspend = adspend.groupby(['campaign']).agg(
    cost=('cost', 'sum'),
    installs=('max_value_installs', 'sum'),
    clicks=('network_clicks','sum')
).reset_index()

revenue_cost  = pd.merge(revenue_x_installs_grouped, adspend, how='left', on=['campaign'])


revenue_cost ['adjusted_cost'] = revenue_cost['cost'] * (revenue_cost['installs_x']/revenue_cost['installs_y'])

revenue_cost_clipped = revenue_cost[revenue_cost['adjusted_cost'] != 0]
avg_cost_per_click = revenue_cost_clipped['adjusted_cost'].sum()/revenue_cost_clipped['clicks'].sum()

revenue_cost['adjusted_cost'] = revenue_cost.apply(
    lambda row: (row['installs_x'] / row['installs_y']) * avg_cost_per_click if row['adjusted_cost'] == 0 else row['adjusted_cost'],
    axis=1
)

revenue_cost['daily_roi'] = revenue_cost['revenue'] / revenue_cost['adjusted_cost']

revenue_cost_without_anomaly = revenue_cost[revenue_cost['daily_roi'] < 1]

revenue_cost_without_anomaly['days_to_payoff'] = 1/(revenue_cost_without_anomaly['daily_roi'])