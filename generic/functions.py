import pandas as pd
import os


def read_data(path):
    data = pd.read_csv(path)
    return data


def join_filenames(path, names):
    file_paths = [os.path.join(path, name) for name in names]
    return file_paths


def create_index(row):
    """
    This function takes channel 1, campaign 30, creative 8 and converts it to an index in the format 1-30-8.
    I ended up not using this in the end, because the join really didn't give me as much joy as I had hoped :(
    """
    # Extract numbers from the strings
    channel_num = row['channel'].split()[1]
    campaign_num = row['campaign'].split()[1]
    creative_num = row['creative'].split()[1]

    # Create the index in the format channel-campaign-creative
    return f"{channel_num}-{campaign_num}-{creative_num}"