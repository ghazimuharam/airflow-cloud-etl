#!/usr/bin/env python3
import os
from src import CreateBucketHelper, CreateDatasetHelper
from google.api_core.exceptions import Conflict
from google.cloud import bigquery

import src
"""
Module Docstring
"""

__author__ = "Muhammad Ghazi Muharam"
__version__ = "0.1.0"
__license__ = "MIT"

project_name = 'linen-patrol-285921'
location = 'US'
dataset_name = 'searched_keyword'
schema = [
    bigquery.SchemaField(
        "user_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("search_keyword",
                         "STRING", mode="REQUIRED"),
    bigquery.SchemaField("search_result_count",
                         "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField(
        "created_at", "STRING", mode="REQUIRED"),
]
bucket_name = 'week2-blank-space'
src_file_path = './src/dataflow'


def main():
    """ Initialize Google Big Query and Google Storage Bucket """

    # # Create Google Big Query Dataset
    Dataset = CreateDatasetHelper(
        project=project_name,
        location=location, dataset_name=dataset_name)
    try:
        Dataset.create_table(table_name='searched_keyword', schema=schema)
    except Conflict:
        print(f'Dataset {dataset_name} Already Exists on {project_name}')

    try:
        Dataset.create_table(table_name='top_searched_keyword', schema=schema)
    except Conflict:
        print(f'Dataset {dataset_name} Already Exists on {project_name}')

    # # Create Google Storage Bucket
    Bucket = CreateBucketHelper(bucket_name=bucket_name)
    try:
        Bucket.create_bucket()
    except Conflict:
        print(f'Bucket {bucket_name} Already Exists on {project_name}')

    for file in os.listdir(src_file_path):
        path_file = src_file_path + '/' + file
        Bucket.upload_blob(path_file, file)


if __name__ == "__main__":
    """ This is executed when run from the command line """
    main()
