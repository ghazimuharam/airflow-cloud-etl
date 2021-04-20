#!/usr/bin/env python3
import os
from src import CreateBucketHelper, CreateDatasetHelper
from google.api_core.exceptions import Conflict
from google.cloud import bigquery
"""
Module Docstring
"""

__author__ = "Muhammad Ghazi Muharam"
__version__ = "0.1.0"
__license__ = "MIT"

# Your project name
project_name = 'linen-patrol-285921'

# Your dataset location
location = 'US'

# Your bucket name
bucket_name = 'week2-blank-space'

# Directory of dataflow operator
# transformers
src_file_path = './src/dataflow'

# Table schema under the same
# dataset share the same keys
dataset_name = {'keyword': 'searched_keyword', 'transaction': 'transaction'}
schema = {'keyword': [
    bigquery.SchemaField(
        "user_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("search_keyword",
                         "STRING", mode="REQUIRED"),
    bigquery.SchemaField("search_result_count",
                         "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField(
        "created_at", "STRING", mode="REQUIRED"),
], 'transaction': [
    bigquery.SchemaField(
        "transaction_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("transaction_detail_id",
                         "INTEGER"),
    bigquery.SchemaField("transaction_number",
                         "STRING"),
    bigquery.SchemaField("transaction_datetime",
                         "DATETIME"),
    bigquery.SchemaField("purchase_quantity",
                         "INTEGER"),
    bigquery.SchemaField("purchase_amount",
                         "FLOAT64"),
    bigquery.SchemaField("purchase_payment_method",
                         "STRING"),
    bigquery.SchemaField("purchase_source",
                         "STRING"),
    bigquery.SchemaField("product_id",
                         "INTEGER"),
    bigquery.SchemaField("user_id",
                         "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("state",
                         "STRING"),
    bigquery.SchemaField("city",
                         "STRING"),
    bigquery.SchemaField(
        "created_at", "DATETIME", mode="REQUIRED"),
    bigquery.SchemaField(
        "ex_created_at", "DATETIME", mode="REQUIRED"),
]}


def create_dataset_keyword():
    # # Create Google Big Query Dataset
    Dataset = CreateDatasetHelper(
        project=project_name,
        location=location, dataset_name=dataset_name['keyword'])
    try:
        Dataset.create_table(table_name='searched_keyword',
                             schema=schema['keyword'])
    except Conflict:
        print(
            f'Table {dataset_name["keyword"]} Already Exists on {project_name}')

    try:
        Dataset.create_table(
            table_name='top_searched_keyword', schema=schema['keyword'])
    except Conflict:
        print(
            f'Table {dataset_name["keyword"]} Already Exists on {project_name}')


def create_dataset_transaction():
    Dataset = CreateDatasetHelper(
        project=project_name,
        location=location, dataset_name=dataset_name['transaction'])
    try:
        Dataset.create_table(
            table_name='transaction', schema=schema['transaction'])
    except Conflict:
        print(
            f'Dataset {dataset_name["transaction"]} Already Exists on {project_name}')


def create_storage_bucket():
    # # Create Google Storage Bucket
    Bucket = CreateBucketHelper(bucket_name=bucket_name)
    try:
        Bucket.create_bucket()
    except Conflict:
        print(f'Bucket {bucket_name} Already Exists on {project_name}')

    for file in os.listdir(src_file_path):
        path_file = src_file_path + '/' + file
        Bucket.upload_blob(path_file, file)


def main():
    """ Initialize Google Big Query and Google Storage Bucket """
    create_dataset_keyword()
    create_dataset_transaction()
    create_storage_bucket()


if __name__ == "__main__":
    """ This is executed when run from the command line """
    main()
