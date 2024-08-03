import pandas as pd
import os


def read_data(path):
    data = pd.read_csv(path)
    return data


def join_filenames(path, names):
    file_paths = [os.path.join(path, name) for name in names]
    return file_paths
