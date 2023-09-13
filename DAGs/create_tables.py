from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime
from airflow.models import Variable

default_args={
    'owner': 'deeep',
    'start_date': datetime(2023, 5, 15)
    }

with DAG('create_bigquery_table', 
         schedule_interval='@once',default_args=default_args) as dag:

    create_img_table_sql = Variable.get('create_img_table_sql')
    create_img_stg_table_sql = Variable.get('create_img_stg_table_sql')
    create_tweet_table_sql = Variable.get('create_tweet_table_sql')
    create_tweet_stg_table_sql=Variable.get('create_tweet_stg_table_sql')
    create_img_senti_scr_table_sql=Variable.get('create_img_senti_scr_table_sql')
    
    create_img_table_task = BigQueryExecuteQueryOperator(
        task_id='create_img_table',
        sql=create_img_table_sql,
        use_legacy_sql=False,
        gcp_conn_id='image-interpreter'
    )
    create_img_stg_table_task = BigQueryExecuteQueryOperator(
        task_id='create_img_stg_table',
        sql=create_img_stg_table_sql,
        use_legacy_sql=False,
        gcp_conn_id='image-interpreter'
    )
    create_tweet_table_task = BigQueryExecuteQueryOperator(
        task_id='create_tweet_table',
        sql=create_tweet_table_sql,
        use_legacy_sql=False,
        gcp_conn_id='image-interpreter'
    )
    create_tweet_stg_table_task = BigQueryExecuteQueryOperator(
        task_id='create_tweet_stg_table',
        sql=create_tweet_stg_table_sql,
        use_legacy_sql=False,
        gcp_conn_id='image-interpreter'
    )

    create_img_senti_scr_table_task = BigQueryExecuteQueryOperator(
        task_id='create_img_senti_scr_table',
        sql=create_img_senti_scr_table_sql,
        use_legacy_sql=False,
        gcp_conn_id='image-interpreter'
    )
    
    create_img_table_task
    create_img_stg_table_task
    create_tweet_table_task
    create_tweet_stg_table_task
    create_img_senti_scr_table_task