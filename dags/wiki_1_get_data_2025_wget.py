from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonVirtualenvOperator


with DAG(
    'wiki_1_get_data_2025_wget',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=1,
    description='wiki get data',
    schedule="10 10 * * *",
    start_date=datetime(2025, 1, 1),
    end_date=datetime(2025, 4, 16),
    catchup=True,
    tags=['wiki','load', 'big'],
) as dag:
    
     # pageviews gz 파일 다운로드
    

    # projectviews 확장자 없는 파일 다운로드
    download_projectviews = BashOperator(
        task_id='download_projectviews_no_ext',
        bash_command="""
        BASE_URL="https://dumps.wikimedia.org/other/pageviews/{{ ds[:4] }}/{{ ds[:4] }}-{{ ds[5:7] }}"
        SAVE_DIR="$HOME/data/wiki/projectviews_wget/{{ ds }}"
        
        echo "BASE_URL: $BASE_URL"
        echo "SAVE_DIR: $SAVE_DIR"
        
        mkdir -p "$SAVE_DIR"
        
        for hour in $(seq -w 0 23); do
          wget -nc "${BASE_URL}/projectviews-{{ ds_nodash }}-${hour}0000" -P $SAVE_DIR
        done
        
        touch "$SAVE_DIR/_DONE"
        echo "projectviews files downloaded to $SAVE_DIR"
        """,
    )
    
    # pageviews gz 파일 다운로드
    download_pageviews = BashOperator(
        task_id='download_pageviews_gz',
        bash_command="""
        BASE_URL="https://dumps.wikimedia.org/other/pageviews/{{ ds[:4] }}/{{ ds[:4] }}-{{ ds[5:7] }}"
        SAVE_DIR="$HOME/data/wiki/pageviews_wget/{{ ds }}"
        
        echo "BASE_URL: $BASE_URL"
        echo "SAVE_DIR: $SAVE_DIR"
        
        mkdir -p "$SAVE_DIR"
        
        for hour in $(seq -w 0 23); do
          wget -nc "${BASE_URL}/pageviews-{{ ds_nodash }}-${hour}0000.gz" -P $SAVE_DIR
        done
        
        touch "$SAVE_DIR/_DONE"
        echo "pageviews files downloaded to $SAVE_DIR"
        """,
    )
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    start >> download_projectviews >> download_pageviews >> end

if __name__ == "__main__":
    dag.test()