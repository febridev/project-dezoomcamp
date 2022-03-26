import os
import logging

from time import time
from kaggle.api.kaggle_api_extended import KaggleApi
import pyarrow.csv as pv
import pyarrow.parquet as pq


def download_from_kaggle():
    dataset = 'yamaerenay/spotify-dataset-19212020-600k-tracks'
    path = 'kaggle'
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset, path,unzip=True)
    # api.dataset_download_file(dataset, 'artists.csv', path,)
    # api.dataset_download_file(dataset, 'tracks.csv', path,unzip=True)

def fhv_csv_to_parquet(srcfile):
    for lsfile in srcfile:
        if not srcfile.endswith('.csv'):
            logging.error("Can only accept source files in CSV format, for the moment")
            return
        table = pv.read_csv(srcfile)
        pq.write_table(table, srcfile.replace('.csv', '.parquet'))



# if __name__ == "__main__":
#     download_from_kaggle()