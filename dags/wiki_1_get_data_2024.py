from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonVirtualenvOperator


with DAG(
    'wiki_1_get_data_2025',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=2,
    max_active_tasks=3,
    description='wiki get data',
    schedule="10 10 * * *",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2025, 1, 1),
    catchup=True,
    tags=['wiki','load', 'big'],
) as dag:
    
     # pageviews gz 파일 다운로드
    

    # projectviews 확장자 없는 파일 다운로드
    download_projectviews = BashOperator(
        task_id='download_projectviews_no_ext',
        bash_command="""
        BASE_URL="https://dumps.wikimedia.org/other/pageviews/{{ ds[:4] }}/{{ ds[:4] }}-{{ ds[5:7] }}"
        SAVE_DIR="$HOME/data/wiki/projectviews/{{ ds }}"
        
        echo "BASE_URL: $BASE_URL"
        echo "SAVE_DIR: $SAVE_DIR"
        
        mkdir -p "$SAVE_DIR"
        for hour in $(seq -w 0 23); do
          echo "${BASE_URL}/projectviews-{{ ds_nodash }}-${hour}0000" >> "$SAVE_DIR/aria2_projectviews.txt"
        done
        aria2c -x 1 -s 1 -j 1 -i "$SAVE_DIR/aria2_projectviews.txt" -d "$SAVE_DIR"
        
        rm "$SAVE_DIR/aria2_projectviews.txt"
        touch "$SAVE_DIR/_DONE"
        echo "projectviews files downloaded to $SAVE_DIR"
        """,
    )
    
    # pageviews gz 파일 다운로드
    download_pageviews = BashOperator(
        task_id='download_pageviews_gz',
        bash_command="""
        BASE_URL="https://dumps.wikimedia.org/other/pageviews/{{ ds[:4] }}/{{ ds[:4] }}-{{ ds[5:7] }}"
        SAVE_DIR="$HOME/data/wiki/pageviews/{{ ds }}"
        
        echo "BASE_URL: $BASE_URL"
        echo "SAVE_DIR: $SAVE_DIR"
        
        mkdir -p "$SAVE_DIR"
        for hour in $(seq -w 0 23); do
          echo "${BASE_URL}/pageviews-{{ ds_nodash }}-${hour}0000.gz" >> "$SAVE_DIR/aria2_pageviews.txt"
        done
        aria2c -x 3 -s 3 -j 1 -i "$SAVE_DIR/aria2_pageviews.txt" -d "$SAVE_DIR"
        
        rm "$SAVE_DIR/aria2_pageviews.txt"
        touch "$SAVE_DIR/_DONE"
        echo "pageviews files downloaded to $SAVE_DIR"
        """,
    )
    
    upload_projectviews = BashOperator(
        task_id='upload.projectviews',
        bash_command="""
        gcloud storage cp -r \
            $HOME/data/wiki/projectviews/{{ ds }} \
            gs://mansour-bucket/wiki/projectviews/{{ ds }}
        """
        )
    
    rm_projectviews = BashOperator(
        task_id='rm.projectviews',
        bash_command="""
        rm -rf \
            $HOME/data/wiki/projectviews/{{ ds }}
        """
        )
    
    upload_pageviews = BashOperator(
        task_id='upload.pageviews',
        bash_command="""
        gcloud storage cp -r \
            $HOME/data/wiki/pageviews/{{ ds }} \
            gs://mansour-bucket/wiki/pageviews/{{ ds }}
        """
        )
    
    rm_pageviews = BashOperator(
        task_id='rm.pageviews',
        bash_command="""
        rm -rf \
            $HOME/data/wiki/pageviews/{{ ds }}
        """
        )
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    start >> download_projectviews >> download_pageviews
    download_projectviews >> upload_projectviews >> rm_projectviews >> end
    download_pageviews >> upload_pageviews >> rm_pageviews >> end

if __name__ == "__main__":
    dag.test()