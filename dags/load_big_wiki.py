from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonVirtualenvOperator


with DAG(
    'load_big_wiki',
    default_args={
        'depends_on_past': True,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=1,
    description='movie',
    schedule="10 10 * * *",
    start_date=datetime(2024, 1,1),
    end_date=datetime(2025, 4, 10),
    catchup=True,
    tags=['wiki','load', 'big'],
) as dag:
    
    load_parquet2big = BashOperator(
        task_id='load.parquet2big',
        bash_command="""
        bq load --source_format=PARQUET \
        --hive_partitioning_mode=AUTO \
        --hive_partitioning_source_uri_prefix=gs://sunsin-bucket/wiki/parquet/ko/  \
        load.wiki \
        "gs://sunsin-bucket/wiki/parquet/ko/date={{ ds }}/*.parquet" \
        """
    )
    
    show_bq_table = BashOperator(
        task_id='show.bq.table',
        bash_command="""
        bq show load.wiki
        """
    )
    
    def check_bq_table_exists(ds_nodash, ds, **kwargs):        
        from google.cloud import bigquery
        from google.cloud.exceptions import NotFound
        
        project_id = "praxis-bond-455400-a4"
        dataset_id = "load"
        table_id = "wiki"
        
        table_full_name = f"{project_id}.{dataset_id}.{table_id}"

        client = bigquery.Client()

        # https://cloud.google.com/bigquery/docs/samples/bigquery-table-exists?hl=ko#bigquery_table_exists-python
        try:
            client.get_table(table_full_name)
            print("Table {} already exists.".format(table_full_name))
        except NotFound:
            print("Table {} is not found.".format(table_full_name))
            return "load.parquet2big"
        
        try:
            query = f"""
                SELECT title FROM {table_full_name}
                WHERE date = '{ds}'
                LIMIT 1
                """
            query_job = client.query(query)
            rows = query_job.result()

            if len(rows) > 0:
                return "show.bq.table"
            else:
                return "load.parquet2big"
        except Exception as e:
            return "load.parquet2big"
    
    check = BranchPythonVirtualenvOperator(
        task_id='check',
        python_callable=check_bq_table_exists,
        requirements=["google-cloud-bigquery"],
        system_site_packages=False,
    )
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    start >> check
    check >> load_parquet2big
    check >> show_bq_table
    load_parquet2big >> end