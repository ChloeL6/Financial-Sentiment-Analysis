import os
import snscrape.modules.twitter as sntwitter
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago 
from airflow import DAG
from airflow.decorators import dag,task
from airflow.sensors.filesystem import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDatasetOperator 
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor  
from work import get_tweets, tweets_transformation, get_tweets_sentiment, get_tweets_sentiment_score
from work import  get_stock , stock_transformation, write_to_file

default_args = {
    'start_date': days_ago(2), # The start date for DAG running. This function allows us to set the start date to two days ago
    'schedule_interval': timedelta(days=1), # How often our DAG will run. After the start_date, airflow waits for the schedule_interval to pass then triggers the DAG run
    'retries': 1, # How many times to retry in case of failure
    'retry_delay': timedelta(minutes=5), # How long to wait before retrying
}

# instantiate a DAG!
with DAG(
    'scrape_tweets_stocks', 
    description='A DAG to scrape data from Twitter and YahooFinance',
    default_args=default_args,
) as dag:

  scrape_twitter = PythonOperator(
    task_id = "scrape_twitter",
    python_callable=get_tweets
  )

  t_tranformation = PythonOperator(
    task_id = "tweets_transformation",
    python_callable=tweets_transformation
  )

  get_sentiment = PythonOperator(
    task_id = "get_sentiment",
    python_callable=get_tweets_sentiment
  )

  get_sentiment_score = PythonOperator(
    task_id = "get_sentiment_score",
    python_callable=get_tweets_sentiment_score
  )

  save_df = PythonOperator(
    task_id="write_to_file",
    python_callable=write_to_file
  )

  scrape_twitter >> t_tranformation >> get_sentiment >> get_sentiment_score >> save_df