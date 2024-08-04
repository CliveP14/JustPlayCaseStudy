import pandas as pd
import os
from generic import functions, paths


def get_files_in_dictionary(names, base_path):
    # Extracts files from path and automatically names them based on the prefix to .csv specified in names as key
    # elements within the dataframes dictionary"""
    filepaths = [os.path.join(base_path, name) for name in names]
    dataframes = {os.path.splitext(os.path.basename(filepath))[0]: pd.read_csv(filepath) for filepath in filepaths}
    return dataframes


def process_revenue_data(data):
    # Converts all dates to date format and coerces non-dates to NaT. Drops any non-date valued rows (1 error)"""
    data['createdAt'] = pd.to_datetime(data['createdAt'], errors='coerce')
    data = data.dropna(subset=['createdAt'])
    return data


def process_installs_data(data):
    # This function creates an index that joins campaign, channel and creative into one string. I didn't end up using
    # this for the join, however this would work better if there was more complete data"""
    data['index'] = data.apply(functions.create_index, axis=1)
    return data


def process_adspend_data(data):
    # For installs data, I assumed that the max of network_installs and installs was the correct installs total for the
    # day. The function then groups by campaign and computes total cost, install count and click count"""
    data['max_value_installs'] = data[['network_installs', 'installs']].max(axis=1)
    data = data.groupby(['campaign']).agg(
        cost=('cost', 'sum'),
        installs=('max_value_installs', 'sum'),
        clicks=('network_clicks', 'sum')
    ).reset_index()
    return data


def merge_revenue_and_installs(revenue, installs):
    # Here I joined the revenue table with installs. I did a right join because I wanted to also keep installs that
    # didn't generate any revenue. I'm assuming this is possible in the current business model and a non-revenue install
    # would be a potential KPI (much like in a conversion funnel)
    #
    # Then I print this data to csv, as I want this data at a user level for some graphing work later on.
    #
    # I also grouped by index, channel, campaign and creative level so I can join with adspend table to compare the
    # revenue generation with cost.

    revenue_x_installs = pd.merge(revenue, installs, how='right', on='userId')
    revenue_x_installs.to_csv('revenue_x_installs.csv')
    revenue_x_installs_grouped = revenue_x_installs.groupby(['index', 'channel', 'campaign', 'creative']).agg(
        installs=('userId', 'count'),
        users=('userId', 'nunique'),
        revenue=('amount', 'sum')
    ).reset_index()
    return revenue_x_installs_grouped


def merge_revenue_adspend(revenue, adspend):
    # Here I join revenue with adspend. I do a left join as I want to retain any revenue generating campaigns for
    # which there is no adspend cost. In the end I had to join on campaign only because this is the only variable that
    # left me with meaningful data. I am not sure if I am missing some manipulation from my side.
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

    revenue.to_csv('revenue.csv')

    # 3. Merge revenue with installs. This function also aggregates to the channel, campaign, and creative level,
    # and outputs a csv for the ungrouped data.
    revenue_x_installs = merge_revenue_and_installs(revenue, installs)

    # 4. Merge with spend data. This function merges on campaign.
    revenue_cost = merge_revenue_adspend(revenue_x_installs, adspend)

    # 5. Since we have a partial day's data, cost is adjusted pro-rata based on installs. This was a made assumption.
    revenue_cost['adjusted_cost'] = revenue_cost['cost'] * (revenue_cost['installs_x'] / revenue_cost['installs_y'])

    # 6. Remove rows where adjusted cost != 0, as these are anomalous. It would be interesting to find out if there's a
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
