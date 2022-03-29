import os
import logging
from google.cloud import storage
from time import time
from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *


def upload_dim_artists():
    engine = create_engine('bigquery://applied-mystery-341809/bq_project_dezoomcamp')
    table = Table('bq_project_dezoomcamp.dim_zones', MetaData(bind=engine), autoload=True)
    print(select([func.count('*')], from_obj=table).scalar())