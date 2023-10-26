from datetime import timedelta, datetime
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator, DataprocDeleteClusterOperator, DataprocCreateClusterOperator, ClusterGenerator

# Default args
default_args = {
    "owner": "andrew",
    "retries": 5,
    "retry_delay": timedelta(minutes=5)
}

# Constants
BUCKET_NAME = "bucket_name"
BUCKET_PREFIX = "bucket_prefix"
DEST_PREFIX = "dest_prefix"
PROJECT_ID = "gcp_project_id"
CLUSTER_NAME = "cluster_name"
TRANSFORM_JOB_FILE_URI = "job_file_uri1"
DELETE_JOB_FILE_URI = "job_file_uri2"
UPDATE_JOB_FILE_URI = "job_file_uri3"
REGION = "region"
ZONE = "zone"
PORT_NUMBER = "port_number"
DRIVER_JAR_URI = "driver_jar_uri"

# Cluster creation constants
CLOUD_SQL_INSTANCE = "cloud_sql_instance_name"
PIP_INIT = "pip_init_script_uri"
CLOUD_SQL_PROXY_INIT = "cloud_sql_proxy_init_script_uri"
METADATA = {
    "enable-cloud-sql-hive-metastore": "false",
    "additional-cloud-sql-instances": f"{PROJECT_ID}:{REGION}:{CLOUD_SQL_INSTANCE}=tcp:{PORT_NUMBER}",
    "PIP_PACKAGES": "psycopg2==2.9.7 google-cloud-secret-manager"
    }
SERVICE_ACCOUNT_SCOPES=[
    "https://www.googleapis.com/auth/sqlservice.admin",
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/cloud.useraccounts.readonly",
    "https://www.googleapis.com/auth/devstorage.read_write",
    "https://www.googleapis.com/auth/logging.write",
    "https://www.googleapis.com/auth/monitoring.write"
]

# PySpark Job configs
UPDATE_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": UPDATE_JOB_FILE_URI,
        "jar_file_uris": [DRIVER_JAR_URI]
    },
}

DELETE_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": DELETE_JOB_FILE_URI,
        "jar_file_uris": [DRIVER_JAR_URI]
    },
}

TRANSFORM_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": TRANSFORM_JOB_FILE_URI,
        'args': [
            "--input_file=" + f"{BUCKET_PREFIX}/{DEST_PREFIX}" + "{{ ti.xcom_pull(task_ids='datagen_to_local', key='file_name') }}",
            "--output_file=" + BUCKET_PREFIX + "{{ ti.xcom_pull(task_ids='datagen_to_local', key='folder_name') }}"
        ],
        "jar_file_uris": [DRIVER_JAR_URI]
    },
}

# Create cluster config
CLUSTER_CONFIG = ClusterGenerator(
    cluster_name=CLUSTER_NAME,
    project_id=PROJECT_ID,
    region = REGION, 
    zone='zone',
    master_machine_type="machine_type",
    master_disk_size=30, 
    master_disk_type="pd-ssd",
    metadata=METADATA,
    image_version="image_version",
    service_account_scopes=SERVICE_ACCOUNT_SCOPES,
    num_workers=0,
    init_actions_uris=[PIP_INIT, CLOUD_SQL_PROXY_INIT],
    storage_bucket="storage_bucket_name",
    gcp_conn_id="conn_name"
).make()


def datagen_to_local(size:int, schema_path: str, output_dir, ti):
    # imports 
    from datetime import datetime
    import pytz

    from pathlib import Path
    import subprocess

    # Create file names
    today = datetime.now(pytz.timezone('America/Toronto')).strftime("%Y-%m-%d-%H%M%S")
    file_extension = ".jsonl"
    file_name = f"listings_{today}"
    full_file_name = file_name + file_extension
    file_path = Path(output_dir + full_file_name)
    return_path = output_dir + full_file_name

    # Push the name of the folder in GCS and the full file name to xcoms
    ti.xcom_push(key="folder_name", value=file_name)
    ti.xcom_push(key="file_name", value=full_file_name)
    ti.xcom_push(key="local_path", value=return_path)

    if not file_path.parent.is_dir():
        file_path.parent.mkdir(parents=True, exist_ok=True)

    synth_command = subprocess.run(['synth', 'generate', f'{schema_path}', '--size', str(size), '--to', f'jsonl:{file_path}', '--random'], check=True)
    print("The exit code was: %d" % synth_command.returncode)


with DAG(
    default_args=default_args,
    dag_id="datagen_dag",
    start_date=pendulum.datetime(2023, 8, 31, 9, 0, 0, tz="America/Toronto"),
    schedule='@daily'
) as dag:
    datagen = PythonOperator(
        task_id="datagen_to_local",
        python_callable=datagen_to_local,
        op_kwargs={'size': 100, 'schema_path': 'real_estate', 'output_dir': 'data/'}
    )

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        bucket=BUCKET_NAME,
        src="{{ ti.xcom_pull(task_ids='datagen_to_local', key='local_path') }}",
        dst=DEST_PREFIX + "{{ ti.xcom_pull(task_ids='datagen_to_local', key='file_name') }}",
        gcp_conn_id="conn_name"
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        gcp_conn_id='conn_name'
        )
    
    spark_transform = DataprocSubmitJobOperator(
        task_id='transform_listings',
        job=TRANSFORM_JOB,
        region=REGION,
        project_id=PROJECT_ID,
        gcp_conn_id="conn_name"
    )

    delete_rows = DataprocSubmitJobOperator(
        task_id='delete_listings',
        job=DELETE_JOB,
        region=REGION,
        project_id=PROJECT_ID,
        gcp_conn_id="conn_name"
    )

    update_rows = DataprocSubmitJobOperator(
        task_id='update_listings',
        job=UPDATE_JOB,
        region=REGION,
        project_id=PROJECT_ID,
        gcp_conn_id="conn_name"
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        gcp_conn_id='conn_name'
    )


    # Workflow
    datagen >> upload_to_gcs >> create_cluster >> spark_transform >> delete_rows >> update_rows >> delete_cluster

