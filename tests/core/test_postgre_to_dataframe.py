import os
import pytest
import psycopg2
from datetime import datetime
from dotenv import load_dotenv,find_dotenv
from pandas import DataFrame, to_datetime

load_dotenv(find_dotenv())

def test_postgre_to_dataframe(pysoni_client_connection_with_envvars):

    expected_data = {'?column?':[1]}

    expected_results = DataFrame(data = expected_data)
    results = pysoni_client_connection_with_envvars.postgre_to_dataframe(query = "SELECT 1")
    
    assert expected_results.equals(results)

def test_postgre_to_dataframe_with_convert_types(pysoni_client_connection_with_envvars):
    filename = 'test_postgre_to_dataframe'
    path = 'tests/fixtures/'

    time_timestamp = [to_datetime('2001-09-28 01:00')]
    time_date = [to_datetime('2001-09-28', format='%Y-%m-%d')]
    expected_data = {'?column?':[1], 'test_timestamp': time_timestamp, 'test_date': time_date}

    expected_results = DataFrame(data = expected_data)
    results = pysoni_client_connection_with_envvars.postgre_to_dataframe(query = filename,
                        convert_types = True, path_sql_script = path)

    assert expected_results.equals(results)


