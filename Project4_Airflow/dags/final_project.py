from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from final_project_operators.create_tables import CreateTablesOperator


from airflow.models import Variable
from udacity.common.final_project_sql_statements import SqlQueries


default_args = {
    'owner': 'hanh pham van',
    'start_date': pendulum.datetime(2018, 11, 1, 0, 0, 0, 0),
    'end_date': pendulum.datetime(2018, 12, 1, 0, 0, 0, 0),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
}

@dag(
    dag_id='sparkify_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
    max_active_runs=1
)
def final_project(*args, **kwargs):

    start_operator = DummyOperator(task_id='Begin_execution')

    create_redshift_tables = CreateTablesOperator(
        task_id='Create_tables',
        redshift_conn_id='redshift'
    )

    # execute_date = kwargs['execution_date']
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',
        redshift_conn_id='redshift',
        aws_conn_id='aws_user',
        s3_bucket='airflow-project-hanhpv',
        # s3_key='log-data/{}/{}/'.format(execute_date.year, execute_date.month)
        s3_key='log-data/',
        region='us-east-1'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        redshift_conn_id='redshift',
        aws_conn_id='aws_user',
        s3_bucket='airflow-project-hanhpv',
        s3_key='song-data/A/A/',
        region='us-east-1'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.user_table_insert,
        table='users'
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.song_table_insert,
        table='songs'
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.artist_table_insert,
        table='artists'
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.time_table_insert,
        table='time'
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        dq_checks=[
            { 'test_sql': 'SELECT COUNT(*) FROM public.songplays WHERE userid IS NULL', 'expected_result': 0 },
            { 'test_sql': 'SELECT COUNT(*) FROM public."time" WHERE weekday IS NULL', 'expected_result': 0 },
            { 'test_sql': 'SELECT COUNT(*) FROM public.artists WHERE name IS NULL', 'expected_result': 0 },
            { 'test_sql': 'SELECT COUNT(*) FROM public.users WHERE first_name IS NULL', 'expected_result': 0 }
            ]
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> create_redshift_tables
    create_redshift_tables >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()