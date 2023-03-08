import os
from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.decorators import dag,task
from airflow.sensors.filesystem import FileSensor
from airflow.hooks.filesystem import FSHook

import pandas as pd
from tqdm import tqdm
import snscrape.modules.twitter as sntwitter
import re
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
import datetime as dt
from datetime import datetime
import yfinance as yf

import logging
import sys
import os
import yaml
from airflow.models import Variable
import time
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import google.auth


# -------------------------------------------
# Set up logging
# -------------------------------------------

# setup logging and logger
logging.basicConfig(format='[%(levelname)-5s][%(asctime)s][%(module)s:%(lineno)04d] : %(message)s',
                    level=logging.INFO,
                    stream=sys.stderr)


# define logger
logger: logging.Logger = logging.getLogger()
logger.setLevel(level=logging.INFO)


def get_this_dir(filepath: str = __file__) -> str:
    """helper function to return this (python) file directory"""
    return os.path.dirname(os.path.abspath(filepath))



# -------------------------------------------
# Load config file
# -------------------------------------------

_default_config_path = os.path.join(get_this_dir(), './config.yml')
CONF_PATH = Variable.get('config_file', default_var=_default_config_path)
config: dict = {}
with open(CONF_PATH) as open_yaml:
    config: dict =  yaml.full_load(open_yaml)
    logger.info(f"loaded configurations file: {CONF_PATH}")

# Set data dir path
_default_data_dir_path = os.path.join(get_this_dir(), '../data')
DATA_DIR = Variable.get('data_dir', default_var=_default_data_dir_path)



# -------------------------------------------
# Setup bigquery client and table definitions
# -------------------------------------------

PROJECT_NAME = config['project']
DATASET_NAME = config['dataset']


_client : bigquery.Client = None
def get_client() -> bigquery.Client:
    """
    returns a bigquery client to the current project
    Returns:
        bigquery.Client: bigquery client
    """
    # check to see if the client has not been initialized
    global _client
    if _client is None:
        # initialize the client
        _client = bigquery.Client(project=PROJECT_NAME)
        logger.info(f"successfully created bigquery client. project={PROJECT_NAME}")
    return _client


def check_bigquery_client():
    """
    task to see if we can successfully create a bigquery client
    """
    # check if $GOOGLE_APPLICATION_CREDENTIALS is set
    google_app_creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if (google_app_creds is None) or (not os.path.exists(google_app_creds)):
        logger.warn("GOOGLE_APPLICATION_CREDENTIALS is not set properly!")
        logger.warn("You most likely have not edited the docker-compose.yaml file correctly. You must restart docker-compose after doing so.")
    # client from dsa_utils.table_definitions module
    logger.info("checking bigquery client")
    client = get_client()
    location = client.location
    logger.info(f"bigquery client is good. {location}")


def get_tweets():
    return

def clean_text(text):
    text = re.sub('@[A-Za-z0–9]+', '', text) #Removing @mentions
    text = re.sub('#', '', text) # Removing '#' hash tag
    text = re.sub('\n', '', text) # Removing \n
    text = re.sub('https?:\/\/\S+', '', text) # Removing hyperlink


def tweets_transformation():
    return


def get_tweets_sentiment(tweet):
    '''
    Utility function to classify sentiment of passed tweet
    using SentimentIntensityAnalyzer's sentiment method
    '''
    # create SentimentIntensityAnalyzer object of passed tweet text
    # return a dict of results
    SIA_obj = SentimentIntensityAnalyzer()
    analysis = SIA_obj.polarity_scores(tweet)

    # decide sentiment as positive, negative and neutral
    if analysis['neg'] > analysis['pos']:
        return 'negative'
    elif analysis['neg'] < analysis['pos']:
        return 'positive'
    else:
        return 'neutral'
    

def get_tweets_sentiment_score(tweet):
    '''
    Utility function to classify sentiment of passed tweet
    using SentimentIntensityAnalyzer's sentiment method
    '''
    # create SentimentIntensityAnalyzer object of passed tweet text
    # return a dict of results
    SIA_obj = SentimentIntensityAnalyzer()
    analysis = SIA_obj.polarity_scores(tweet)

    # decide sentiment as positive, negative and neutral
    if analysis['neg'] > analysis['pos']:
        return analysis['neg']
    elif analysis['neg'] < analysis['pos']:
        return analysis['pos']
    else:
        return analysis['neu']


def get_stock():
    return


def stock_transformation():
    return


# Define table schemas
TWITTER_SCHEMA = [
    bigquery.SchemaField("date", "date", mode="REQUIRED"),
    bigquery.SchemaField("id", "integer", mode="REQUIRED"),
    bigquery.SchemaField("content", "string", mode="REQUIRED"),
    bigquery.SchemaField("user", "string", mode="REQUIRED"),
    bigquery.SchemaField("sentiment", "string", mode="REQUIRED"),
    bigquery.SchemaField("score", "float", mode="REQUIRED"),
    bigquery.SchemaField("year", "integer", mode="REQUIRED"),
    bigquery.SchemaField("month", "integer", mode="REQUIRED"),
    bigquery.SchemaField("day", "integer", mode="REQUIRED")
]

STOCK_SCHEMA = [
    bigquery.SchemaField("date", "date", mode="REQUIRED"),
    bigquery.SchemaField("open", "float", mode="REQUIRED"),
    bigquery.SchemaField("high", "float", mode="REQUIRED"),
    bigquery.SchemaField("low", "float", mode="REQUIRED"),
    bigquery.SchemaField("close", "float", mode="REQUIRED"),
    bigquery.SchemaField("volume", "integer", mode="REQUIRED"),
    bigquery.SchemaField("year", "integer", mode="REQUIRED"),
    bigquery.SchemaField("month", "integer", mode="REQUIRED"),
    bigquery.SchemaField("day", "integer", mode="REQUIRED"),
]

# global dict to hold all tables schemas
TABLE_SCHEMAS = {
    'twitter': TWITTER_SCHEMA,
    'stock': STOCK_SCHEMA}

def create_table(table_name: str) -> None:
    assert table_name in TABLE_SCHEMAS, f"Table schema not found for table name: {table_name}"

    client = get_client()
    table_id = f"{PROJECT_NAME}.{DATASET_NAME}.{table_name}"

    try:
        table = client.get_table(table_id)      # table exists if this line doesn't raise exception
        client.delete_table(table)
        logger.info(f"dropped existed bigquery table: {table_id}")

        # wait a couple seconds before creating the table again
        time.sleep(2.0)
    except NotFound:
        # it's OK! table didn't exist
        pass
    # create the table
    schema = TABLE_SCHEMAS[table_name]
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table, exists_ok=False)
    logger.info(f"created bigquery table: {table_id}")


# global variable to hold data file
DATA_FILES = { 
    'twitter': os.path.join(DATA_DIR, config['twitter_data']),
    'stock': os.path.join(DATA_DIR, config['stock_data'])}

# load table function 
def load_table(table_name: str):

    # make sure table_name is one of our data files
    assert table_name in DATA_FILES, f"Unknown table name: {table_name}"
    
    client = get_client()
    data_file = DATA_FILES[table_name]

    # check to see if data file exists
    assert os.path.exists(data_file), f"Missing data file: {data_file}"

    # insert data into bigquery
    table_id = f"{PROJECT_NAME}.{DATASET_NAME}.{table_name}"

    # bigquery job config to load from a dataframe
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        create_disposition='CREATE_NEVER',
        write_disposition='WRITE_TRUNCATE',
        max_bad_records=100,
        ignore_unknown_values=True,
    )
    logger.info(f"loading bigquery {table_name} from file: {data_file}")
    with open(data_file, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
    # wait for the job to complete
    job.result()
    # get the number of rows inserted
    table = client.get_table(table_id)
    logger.info(f"inserted {table.num_rows} rows to {table_id}")