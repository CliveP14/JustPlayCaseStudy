import pandas as pd
from generic import functions, paths
import os


def get_files_in_dictionary():
    names = ['adspend.csv', 'installs.csv', 'revenue.csv']
    filepaths = functions.join_filenames(paths.SUBDIRFILES, names)
    dataframes = {}
    for filepath in filepaths:
        key = os.path.splitext(os.path.basename(filepath))[0]
        dataframes[key] = pd.read_csv(filepath)
    return dataframes


dataframes = get_files_in_dictionary()
