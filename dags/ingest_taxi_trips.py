# import needed packages
from airflow import DAG, configuration
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from utils.parquet_to_postgres import fetch_and_load
from utils.branch_decider import is_data_exist

# DAG default arguments
default_args = {
    'depends_on_past': False,
    'email': ['okzamahendra29@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'execution_timeout': timedelta(minutes=10),
}

# definition of dag
dag = DAG(
    dag_id='ingest_taxi_trips',
    # for testing purpose, will be run at 2021-01-01 later
    start_date=datetime(2021, 1, 1),
    end_date=datetime(2022, 1, 1), # to cover at data that arrives after 2021-12-31 23:00:00 UTC
    default_args=default_args,
    schedule_interval='00 23 * * *',  # UTC based
    catchup=True,
    tags=[
        'taxi-trips',
        'parquet',
        'daily',
        'postgres'
    ],
    concurrency=1,
    max_active_runs=1
)

start = EmptyOperator(
    task_id='start',
    dag=dag
)

extract_load_taxi_trips_data = PythonOperator(
    task_id='extract_load_taxi_trips_data',
    python_callable=fetch_and_load,
    op_kwargs={"base_path": configuration.get_airflow_home(
    ), "execution_date": "{{ execution_date | ds }}",
        "url": "{{ var.value.get('postgres_url', None) }}",
        "start_date": "{{ data_interval_start.format('YYYY-MM-DD HH:mm:ss') }}",
        "end_date": "{{ data_interval_end.format('YYYY-MM-DD HH:mm:ss') }}"},
    dag=dag
)

create_raw_trip_data_table_if_not_exists = PostgresOperator(
    task_id='create_raw_trip_data_table_if_not_exists',
    postgres_conn_id='taxi_trips_conn_id',
    sql='sql/create_raw_trip_data_table.sql',
    dag=dag
)

create_daily_trips_table_if_not_exists = PostgresOperator(
    task_id='create_daily_trips_table_if_not_exists',
    postgres_conn_id='taxi_trips_conn_id',
    sql='sql/create_daily_trips_table.sql',
    dag=dag    
)

create_monthly_trips_table_if_not_exists = PostgresOperator(
    task_id='create_monthly_trips_table_if_not_exists',
    postgres_conn_id='taxi_trips_conn_id',
    sql='sql/create_monthly_trips_table.sql',
    dag=dag    
)

check_raw_trip_data_exist_on_exec_date = BranchPythonOperator(
    task_id='check_raw_trip_data_exist_on_exec_date',
    python_callable=is_data_exist,
    op_kwargs={"sql": """
        SELECT COUNT(*) FROM public.raw_trip_data
        WHERE tpep_pickup_datetime >= '{{ data_interval_start.format("YYYY-MM-DD HH:mm:ss") }}'
            AND tpep_pickup_datetime < '{{ data_interval_end.format("YYYY-MM-DD HH:mm:ss") }}'
    """},
    dag=dag
)

insert_stg_raw_trip_data_to_prod = PostgresOperator(
    task_id='insert_stg_raw_trip_data_to_prod',
    postgres_conn_id='taxi_trips_conn_id',
    sql='sql/insert_stg_raw_trip_data_to_prod.sql',
    dag=dag
)

no_insert_data = EmptyOperator(
    task_id='no_insert_data',
    dag=dag
)

full_load_daily_trips_table = PostgresOperator(
    task_id='full_load_daily_trips_table',
    postgres_conn_id='taxi_trips_conn_id',
    sql='sql/full_load_daily_trips.sql',
    dag=dag
)

full_load_mothly_trips_table = PostgresOperator(
    task_id='full_load_mothly_trips_table',
    postgres_conn_id='taxi_trips_conn_id',
    sql='sql/full_load_monthly_trips.sql',
    dag=dag
)

bridger = EmptyOperator(
    task_id='bridger_after_etl',
    dag=dag
)

delete_stg_raw_trip_data_table = PostgresOperator(
    task_id='delete_stg_raw_trip_data_table',
    postgres_conn_id='taxi_trips_conn_id',
    sql='sql/drop_stg_table.sql',
    dag=dag,
    trigger_rule=TriggerRule.ONE_SUCCESS
)
end = EmptyOperator(
    task_id='end',
    dag=dag,
    trigger_rule=TriggerRule.ONE_SUCCESS
)

# tasks's depedency
start >> extract_load_taxi_trips_data
extract_load_taxi_trips_data >> [create_raw_trip_data_table_if_not_exists, create_daily_trips_table_if_not_exists, create_monthly_trips_table_if_not_exists]
[create_raw_trip_data_table_if_not_exists, create_daily_trips_table_if_not_exists, create_monthly_trips_table_if_not_exists] >> check_raw_trip_data_exist_on_exec_date

check_raw_trip_data_exist_on_exec_date >> [insert_stg_raw_trip_data_to_prod, no_insert_data]
insert_stg_raw_trip_data_to_prod >> [full_load_daily_trips_table, full_load_mothly_trips_table]
[full_load_daily_trips_table, full_load_mothly_trips_table] >> bridger

bridger >> delete_stg_raw_trip_data_table
no_insert_data >> delete_stg_raw_trip_data_table

delete_stg_raw_trip_data_table >> end
