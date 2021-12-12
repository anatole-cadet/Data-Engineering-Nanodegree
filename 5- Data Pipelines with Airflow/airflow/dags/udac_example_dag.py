from abc import abstractproperty
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from sqlalchemy.sql.expression import table


from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import  DataQualityOperator

from helpers.sql_queries import SqlQueries



LIST_TABLES_FOR_QUALITY_CHECK = ["time", "artists", "songs", "users", "songplays"]

DQ_CHECKS = [
    {"table":"time",       "expected_result":True},
    {"table":"artists",    "expected_result":True},
    {"table":"songs",      "expected_result":True},
    {"table":"users",      "expected_result":True},
    {"table":"songplays",  "expected_result":True}
]


# 1: insert
# 2: delete_insert
LIST_OPERATIONS_TYPE = [1, 2]


query_object = SqlQueries()

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 12, 8),
    'catchup':False,
    'retries':3,
    'depends_on_past':False,
    'retry_delay':timedelta(minutes=5)
}



def verify_quality_check(query):
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records(query)
    return records



dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket= "s3://udacity-dend/log_data",
    s3_key="s3://udacity-dend/log_json_path.json",
    redshift_conn_id="redshift",
    dag=dag
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket= "s3://udacity-dend", #/song_data",
    s3_key="s3://udacity-dend/song_data", #/A/A/A/*.json",
    redshift_conn_id="redshift",
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    table= "songplays",
    query_fact= query_object.songplay_table_insert,
    conn_id="redshift",
    operation_type=LIST_OPERATIONS_TYPE[1],
    dag=dag
    )


load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    table="users",
    query_dim=query_object.user_table_insert,
    conn_id="redshift",
    operation_type=LIST_OPERATIONS_TYPE[1],
    dag=dag
)


load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    table="songs",
    query_dim=query_object.song_table_insert,
    conn_id="redshift",
    operation_type=LIST_OPERATIONS_TYPE[1],
    dag=dag
)


load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    table="artists",
    query_dim=query_object.artist_table_insert,
    conn_id="redshift",
    operation_type=LIST_OPERATIONS_TYPE[1],
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    table="time",
    query_dim=query_object.time_table_insert,
    conn_id="redshift",
    operation_type=LIST_OPERATIONS_TYPE[1],
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    data_quality_checks = DQ_CHECKS,
    conn_id="redshift",
    dag=dag
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)





# Build the DAG dependencies

start_operator >>  stage_songs_to_redshift
start_operator >>  stage_events_to_redshift


stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table



load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_time_dimension_table
load_songplays_table >> load_user_dimension_table
                            

load_artist_dimension_table  >> run_quality_checks
load_song_dimension_table  >> run_quality_checks
load_time_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
          

run_quality_checks >> end_operator



