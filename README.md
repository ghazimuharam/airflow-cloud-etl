# Airflow Cloud ETL

Airflow Cloud ETL is a simple Data Pipeline repository using Airflow and Google Cloud for Cloud ETL and Batch Processing.

![image](https://user-images.githubusercontent.com/22569688/115386135-3869ee00-a203-11eb-9b1d-7d7cd862ecdc.png)

## Installation

Use git to clone this repository

```bash
git clone https://github.com/ghazimuharam/airflow-cloud-etl.git
```

## Prerequisite

Make sure you have python 3.7 installed on your machine

```bash
> python --version
Python 3.7.10
```

To run the script in this repository, you need to install the prerequisite library from requirements.txt

```bash
pip install -r requirements.txt
```

Store all your service account json files to `./conf` directory

## Usage

Before running Airflow DAGs, you have to configure Airflow Variables and Airflow Connections

### Airflow

To run airflow docker container, use Initializing Environment step on this [Airflow Official Page](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#initializing-environment), make sure you have docker-compose installed on your machine.

#### Airflow Variables

![image](https://user-images.githubusercontent.com/22569688/115381926-49fcc700-a1fe-11eb-904a-2a37eda3fad8.png)

Change `gce_region, gce_zone, project_id, transaction_output_table_id` value with your configuration

#### Airflow Connections

![image](https://user-images.githubusercontent.com/22569688/115382381-d0190d80-a1fe-11eb-9d16-babca763612a.png)

Change

- Keyfile Path to `/opt/airflow/conf/key.json`
- Project Id to `your-project-name`

To run `composer_transaction_integrator` DAG, you have to configure 2 service account json files.

- json_input_config_files (Where the big query stored)
- json_output_config_files (Where to output the processed big query execution)

```python
transaction_integration = IntegrateTransactionOperator(
        task_id='integrate_transaction',
        json_input_config_files="/opt/airflow/conf/bigquery.json",
        json_output_config_files="/opt/airflow/conf/key.json",
        ...
```

### Main

The main application program will create 2 Dataset, 3 Table, and 1 Bucket. The main application program only executed once when initializing the Google Cloud Storage requirements. Before running the main program, run the command below

```bash
export GOOGLE_APPLICATION_CREDENTIALS="./path/to/file.json"
```

Change `./main.py` cloud configuration

```python
# Your project name
project_name = 'linen-patrol-285921'

# Your dataset location
location = 'US'

# Your bucket name
bucket_name = 'week2-blank-space'
```

After setting up the configuration, you can run `./main.py` using command below

```bash
python main.py
```

## License

[MIT](https://choosealicense.com/licenses/mit/)
