import pandas as pd
import os
from generic import functions, paths


def get_files_in_dictionary(names, base_path):
    """Extracts files from path and automatically names them based on the prefix to .csv specified in names as key
    elements within the dataframes dictionary"""
    filepaths = [os.path.join(base_path, name) for name in names]
    dataframes = {os.path.splitext(os.path.basename(filepath))[0]: pd.read_csv(filepath) for filepath in filepaths}
    return dataframes


def process_revenue_data(data):
    data['createdAt'] = pd.to_datetime(data['createdAt'], errors='coerce')
    data = data.dropna(subset=['createdAt'])
    return data


def process_installs_data(data):
    data['index'] = data.apply(functions.create_index, axis=1)
    return data


def process_adspend_data(data):
    data['max_value_installs'] = data[['network_installs', 'installs']].max(axis=1)
    data = data.groupby(['campaign']).agg(
        cost=('cost', 'sum'),
        installs=('max_value_installs', 'sum'),
        clicks=('network_clicks', 'sum')
    ).reset_index()
    return data


def merge_revenue_and_installs(revenue, installs):
    revenue_x_installs = pd.merge(revenue, installs, how='right', on='userId')
    revenue_x_installs_grouped = revenue_x_installs.groupby(['index', 'channel', 'campaign', 'creative']).agg(
        installs=('userId', 'count'),
        users=('userId', 'nunique'),
        revenue=('amount', 'sum')
    ).reset_index()
    return revenue_x_installs_grouped


def merge_revenue_adspend(revenue, adspend):
    revenue_cost = pd.merge(revenue, adspend, how='left', on=['campaign'])
    return revenue_cost


def main():
    # 1. load all dataframes to a dictionary
    base_path = paths.SUBDIRFILES
    filenames = ['adspend.csv', 'installs.csv', 'revenue.csv']
    dataframes = get_files_in_dictionary(filenames, base_path)

    # 2. process each dataframe by removing errors, unwanted rows, etc.
    revenue = process_revenue_data(dataframes['revenue'])
    installs = process_installs_data(dataframes['installs'])
    adspend = process_adspend_data(dataframes['adspend'])

    # 3. Merge revenue with installs. This function also aggregates to the channel, campaign, and creative level.
    revenue_x_installs = merge_revenue_and_installs(revenue, installs)

    # 4. Merge with spend data. This function merges on campaign.
    revenue_cost = merge_revenue_adspend(revenue_x_installs, adspend)

    # 5. Since we have a partial day's data, cost is adjusted pro-rata based on installs. This was a made assumption.
    revenue_cost['adjusted_cost'] = revenue_cost['cost'] * (revenue_cost['installs_x'] / revenue_cost['installs_y'])

    # 6. Remove rows where adjusted cost != 0, as these are anomalous. I would be interesting to find out if there's a
    # method or algorithm that can be used to impute these in the future. Further, it would be useful to have a separate
    # table that defines how each campaign/creative/channel is costed (aka, per click, revenue sharing, etc.)
    revenue_cost = revenue_cost[revenue_cost['adjusted_cost'] != 0]

    # 7. Calculate ROI and annualize it
    revenue_cost['daily_roi'] = revenue_cost['revenue'] / revenue_cost['adjusted_cost']
    revenue_cost['roi'] = revenue_cost['daily_roi'] * 365

    # 8. Push to csv
    revenue_cost.to_csv('revenue_cost.csv')

    return revenue_cost


if __name__ == "__main__":
    revenue_cost = main()
