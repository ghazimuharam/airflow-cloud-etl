from datetime import datetime, timedelta
import json
from airflow.models.baseoperator import BaseOperator
from airflow.operators.python import PythonOperator
from airflow import models
from google.cloud import bigquery
import pandas as pd

transaction_output_table_id = models.Variable.get(
    "transaction_output_table_id")


class IntegrateTransactionOperator(BaseOperator):
    """
    """

    def __init__(self,
                 json_input_config_files,
                 query,
                 output_table_id,
                 json_output_config_files=None,
                 *args, **kwargs) -> None:

        super().__init__(*args, **kwargs)
        self.client = bigquery.Client.from_service_account_json(
            json_input_config_files)
        self.query = query
        self.output_table_id = output_table_id

        # This code will be executed if you have
        # different input and output bigquery
        # service account
        if json_output_config_files is not None:
            self.out_client = bigquery.Client.from_service_account_json(
                json_output_config_files)
        else:
            self.out_client = self.client

    def query_executor(self):
        """
        """

        query_job = self.client.query(
            self.query
        )

        results = query_job.result()  # Waits for job to complete.

        return results

    def execute(self, context):
        self.query = self.query.format(context.get("execution_date"))

        df = self.query_executor().to_dataframe()

        for row_dict in df.to_dict(orient="records"):
            transaction = {}

            event_params = {items['key']: items['value']
                            for items in row_dict['event_params']}

            try:
                transaction['transaction_id'] = int(
                    event_params['transaction_id']['int_value'])
            except KeyError:
                continue

            transaction['transaction_detail_id'] = int(
                event_params['transaction_detail_id']['int_value'])

            transaction['transaction_number'] = event_params['transaction_number']['string_value']

            transaction['transaction_datetime'] = str(
                row_dict['event_datetime'])

            transaction['purchase_quantity'] = int(
                event_params['purchase_quantity']['int_value'])

            transaction['purchase_amount'] = event_params['purchase_amount']['float_value']

            transaction['purchase_payment_method'] = event_params['purchase_payment_method']['string_value']

            try:
                transaction['purchase_source'] = event_params['purchase_source']['string_value']
            except TypeError:
                transaction['purchase_source'] = ''

            transaction['product_id'] = int(
                event_params['product_id']['int_value'])

            transaction['user_id'] = row_dict['user_id']

            transaction['state'] = row_dict['state']

            transaction['city'] = row_dict['city']

            transaction['created_at'] = str(pd.Timestamp.now(tz='utc'))

            transaction['ext_created_at'] = str(row_dict['created_at'])

            errors = self.out_client.insert_rows_json(
                self.output_table_id, [transaction])
            if errors != []:
                print("Encountered errors while inserting rows: {}".format(errors))
                print(transaction)


default_args = {
    "start_date": datetime(2021, 3, 21),
    "depends_on_past": True,
}

# Define a DAG (directed acyclic graph) of tasks.
with models.DAG(
    # The id you will see in the DAG airflow page
    "composer_transaction_integrator",
    default_args=default_args,
    # The interval with which to schedule the DAG
    schedule_interval=timedelta(
        days=3),  # Override to match your needs
) as dag:

    transaction_integration = IntegrateTransactionOperator(
        task_id='integrate_transaction',
        json_input_config_files="/opt/airflow/conf/bigquery.json",
        json_output_config_files="/opt/airflow/conf/key.json",
        output_table_id=transaction_output_table_id,
        query="""
        SELECT
          * 
        FROM `pkl-playing-fields.unified_events.event` 
        WHERE event_name="purchase_item" AND event_datetime > timestamp('{0}') AND event_datetime < timestamp_add(timestamp('{0}'), INTERVAL 3 DAY) ORDER BY event_datetime""")
