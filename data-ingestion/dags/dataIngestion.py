import os
import logging

from time import time
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq



def download_from_kaggle(downloadpath):
    dataset = 'yamaerenay/spotify-dataset-19212020-600k-tracks'
    path = downloadpath
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset, path,unzip=True)
    # api.dataset_download_file(dataset, 'artists.csv', path,)
    # api.dataset_download_file(dataset, 'tracks.csv', path,unzip=True)

def fhv_csv_to_parquet(srcfile):
    list_csv_file = os.listdir(srcfile)
    for lsfile in list_csv_file:
        if lsfile.endswith(".csv"):
            fullcsv = f'{srcfile}/{lsfile}'
            # print(fullcsv)
            if not fullcsv.endswith('.csv'):
                logging.error("Can only accept source files in CSV format, for the moment")
                return
            table = pv.read_csv(fullcsv)
            pq.write_table(table, fullcsv.replace('.csv', '.parquet'))

def upload_to_gbucket(bucket,srcfile):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)
    list_parquet_file = os.listdir(srcfile)
    for lsfile in list_parquet_file:
        if lsfile.endswith(".csv"):
            fullparquet=f'{srcfile}/{lsfile}'
            bucketpath=f'raw/{lsfile}'
            # print(bucket)
            # print(fullparquet)
            # print(bucketpath)
            blob = bucket.blob(bucketpath)
            blob.upload_from_filename(fullparquet)

# if __name__ == "__main__":
#     download_from_kaggle()