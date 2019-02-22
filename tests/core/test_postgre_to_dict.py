import os
import pytest
import psycopg2
from datetime import datetime
from dotenv import load_dotenv,find_dotenv
from pandas import DataFrame, to_datetime

load_dotenv(find_dotenv())

def test_postgre_to_dict(pysoni_client_connection_with_envvars):
    
    expected_results = [[{'column1': 1}, {'col2': 2}, {'?column?': 3}]]
    results = pysoni_client_connection_with_envvars.postgre_to_dict(
        query="SELECT 1 AS column1, 2 AS col2, 3")
    
    assert expected_results == results

def test_postgre_to_dict_with_types_and_script(
        pysoni_client_connection_with_envvars):

    filename = 'test_postgre_to_dict'
    path = 'tests/fixtures/'
    
    expected_results = [[{'mynum': {'value': 1, 'type': 'int4'}}, 
        {'test_date': {'value': datetime.date(datetime(2001, 9, 28)), 
                        'type': 'date'}}]]
    results = pysoni_client_connection_with_envvars.postgre_to_dict(
        query=filename, types = True, path_sql_script=path)
    
    assert expected_results == results
    