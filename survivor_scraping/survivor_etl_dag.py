import sys
sys.path.append('/home/pi/airflow/dags/survivor_scraping')


from sqlalchemy import create_engine
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
import os
from survivor_processing.src.survivor_scraping.season.season_load import load_seasons
from survivor_processing.src.survivor_scraping.season.season_transform import transform_seasons
from survivor_processing.src.survivor_scraping.season.season_extract import extract_seasons
from survivor_processing.src.survivor_scraping.reddit.reddit_load import load_reddit
from survivor_processing.src.survivor_scraping.reddit.reddit_transform import transform_reddit
from survivor_processing.src.survivor_scraping.reddit.reddit_extract import extract_reddit
from survivor_processing.src.survivor_scraping.episodes.episodes_load import load_episodes
from survivor_processing.src.survivor_scraping.episodes.episodes_transform import transform_episodes
from survivor_processing.src.survivor_scraping.episodes.episodes_extract import extract_episodes
from survivor_processing.src.survivor_scraping.episode_stats.episode_stats_load import load_episode_stats
from survivor_processing.src.survivor_scraping.episode_stats.episode_stats_transform import transform_episode_stats
from survivor_processing.src.survivor_scraping.episode_stats.episode_stats_extract import extract_episode_stats
from survivor_processing.src.survivor_scraping.contestant.contestant_load import load_contestants
from survivor_processing.src.survivor_scraping.contestant.contestant_transform import transform_contestants
from survivor_processing.src.survivor_scraping.contestant.contestant_extract import extract_contestants
from survivor_processing.src.survivor_scraping.confessional.confessional_load import load_confessionals
from survivor_processing.src.survivor_scraping.confessional.confessional_transform import transform_confessionals
from survivor_processing.src.survivor_scraping.confessional.confessional_extract import extract_confessionals
from airflow.hooks.base_hook import BaseHook

connection = BaseHook.get_connection('postgres_default')
pg_creds = connection


def create_engine_from_connection(connection):

    format_dict = {
        'username': connection.login,
        'password': connection.password,
        'host': connection.host,
        'port': connection.port
    }
    con_str = 'postgresql://{username}:{password}@{host}:{port}'.format(
        **format_dict)
    return create_engine(con_str)


PARAMS = dict(connection=pg_creds)


def etl_confessional(connection, *args, **kwargs):
    eng = create_engine_from_connection(connection)
    ds = kwargs.get('asof', kwargs['ds'])

    e = extract_confessionals(eng, asof=ds)
    t = transform_confessionals(eng, e)
    load_confessionals(t, eng)


def etl_contestants(connection, *args, **kwargs):
    eng = create_engine_from_connection(connection)
    ds = kwargs.get('asof', kwargs['ds'])

    e = extract_contestants(eng, asof=ds)
    t = transform_contestants(eng, e)
    load_contestants(t, eng)


def etl_ep_stats(connection, *args, **kwargs):
    eng = create_engine_from_connection(connection)
    ds = kwargs.get('asof', kwargs['ds'])

    e = extract_episode_stats(eng, asof=ds)
    t = transform_episode_stats(eng, e)
    load_episode_stats(t, eng)


def etl_episodes(connection, *args, **kwargs):
    eng = create_engine_from_connection(connection)
    ds = kwargs.get('asof', kwargs['ds'])

    e = extract_episodes(eng, asof=ds)
    t = transform_episodes(eng, e)
    load_episodes(t, eng)


def etl_reddit(connection, *args, **kwargs):
    eng = create_engine_from_connection(connection)
    ds = kwargs.get('asof', kwargs['ds'])

    e = extract_reddit(eng, asof=ds)
    t = transform_reddit(eng, e)
    load_reddit(t, eng)


def etl_seasons(connection, *args, **kwargs):
    eng = create_engine_from_connection(connection)
    ds = kwargs.get('asof', kwargs['ds'])

    e = extract_seasons(eng, asof=ds)
    t = transform_seasons(eng, e)
    load_seasons(t, eng)


daily_dag = DAG('survivor_etl_daily',
                description='ETL Pipeline for Survivor Related Data for most recent season',
                schedule_interval="@daily",
                start_date=datetime(2020, 7, 10), catchup=False)

weekly_dag = DAG('survivor_etl_weekly',
                 description='ETL Pipeline for Survivor Related Data for last 2 year seasons',
                 schedule_interval="@weekly",
                 start_date=datetime(2020, 7, 10), catchup=False)

daily_dag_params = {}
weekly_dag_params = {'asof': datetime.now() - timedelta(days=700)}

daily_dag_params.update(PARAMS)
weekly_dag_params.update(PARAMS)

daily_etl_confessional = PythonOperator(
    task_id='etl_confessional', provide_context=True, op_kwargs=daily_dag_params,
    python_callable=etl_confessional, dag=daily_dag)
daily_etl_contestant = PythonOperator(
    task_id='etl_contestant', provide_context=True, op_kwargs=daily_dag_params,
    python_callable=etl_contestants, dag=daily_dag)
daily_etl_ep_stats = PythonOperator(
    task_id='etl_ep_stats', provide_context=True, op_kwargs=daily_dag_params,
    python_callable=etl_ep_stats, dag=daily_dag)
daily_etl_episodes = PythonOperator(
    task_id='etl_episodes', provide_context=True, op_kwargs=daily_dag_params,
    python_callable=etl_episodes, dag=daily_dag)
daily_etl_reddit = PythonOperator(
    task_id='etl_reddit', provide_context=True, op_kwargs=daily_dag_params,
    python_callable=etl_reddit, dag=daily_dag)
daily_etl_seasons = PythonOperator(
    task_id='etl_seasons', provide_context=True, op_kwargs=daily_dag_params,
    python_callable=etl_seasons, dag=daily_dag)


weekly_etl_confessional = PythonOperator(
    task_id='etl_confessional', provide_context=True, op_kwargs=weekly_dag_params,
    python_callable=etl_confessional, dag=weekly_dag)
weekly_etl_contestant = PythonOperator(
    task_id='etl_contestant', provide_context=True, op_kwargs=weekly_dag_params,
    python_callable=etl_contestants, dag=weekly_dag)
weekly_etl_ep_stats = PythonOperator(
    task_id='etl_ep_stats', provide_context=True, op_kwargs=weekly_dag_params,
    python_callable=etl_ep_stats, dag=weekly_dag)
weekly_etl_episodes = PythonOperator(
    task_id='etl_episodes', provide_context=True, op_kwargs=weekly_dag_params,
    python_callable=etl_episodes, dag=weekly_dag)
weekly_etl_reddit = PythonOperator(
    task_id='etl_reddit', provide_context=True, op_kwargs=weekly_dag_params,
    python_callable=etl_reddit, dag=weekly_dag)
weekly_etl_seasons = PythonOperator(
    task_id='etl_seasons', provide_context=True, op_kwargs=weekly_dag_params,
    python_callable=etl_seasons, dag=weekly_dag)

daily_etl_seasons >> [daily_etl_contestant,
                      daily_etl_episodes] >> daily_etl_ep_stats
daily_etl_seasons >> [daily_etl_contestant,
                      daily_etl_episodes] >> daily_etl_reddit
daily_etl_seasons >> [daily_etl_contestant,
                      daily_etl_episodes] >> daily_etl_confessional

weekly_etl_seasons >> [weekly_etl_contestant,
                       weekly_etl_episodes] >> weekly_etl_ep_stats
weekly_etl_seasons >> [weekly_etl_contestant,
                       weekly_etl_episodes] >> weekly_etl_reddit
weekly_etl_seasons >> [weekly_etl_contestant,
                       weekly_etl_episodes] >> weekly_etl_confessional
