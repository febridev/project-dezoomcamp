import os
import logging

from time import time
from kaggle.api.kaggle_api_extended import KaggleApi
 

def download_from_kaggle():
    dataset = 'yamaerenay/spotify-dataset-19212020-600k-tracks'
    path = 'kaggle'
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset, path,unzip=True)
    # api.dataset_download_file(dataset, 'artists.csv', path,)
    # api.dataset_download_file(dataset, 'tracks.csv', path,unzip=True)

# if __name__ == "__main__":
#     download_from_kaggle()