from google.cloud import bigquery


class CreateDatasetHelper:

    def __init__(self, project, location, dataset_name) -> None:
        self.project = project  # Your GCP Project
        self.location = location  # the location where you want your BigQuery data to reside. For more info on possible locations see https://cloud.google.com/bigquery/docs/locations
        self.dataset_name = dataset_name  # Dataset name
        self.client = bigquery.Client(self.project)

        try:
            self.create_dataset()
        except:
            pass

    def create_dataset(self):
        dataset_id = f"{self.project}.{self.dataset_name}"

        # Construct a full Dataset object to send to the API.
        dataset = bigquery.Dataset(dataset_id)

        # Set the location to your desired location for the dataset.
        # For more information, see this link:
        # https://cloud.google.com/bigquery/docs/locations
        dataset.location = self.location

        # Send the dataset to the API for creation.
        # Raises google.api_core.exceptions.Conflict if the Dataset already
        # exists within the project.
        dataset = self.client.create_dataset(dataset)  # Make an API request.

        print(f"Created dataset {self.client.project}.{dataset.dataset_id}")

    def create_table(self, table_name, schema):
        # Create a table from this dataset.

        table_id = f"{self.project}.{self.dataset_name}.{table_name}"

        table = bigquery.Table(table_id, schema=schema)
        table = self.client.create_table(table)  # Make an API request.
        print(
            f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
