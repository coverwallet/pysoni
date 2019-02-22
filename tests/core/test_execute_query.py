import os
import pytest
import psycopg2
from dotenv import load_dotenv,find_dotenv

load_dotenv(find_dotenv())

def test_execute_query(pysoni_client_connection_with_envvars):
    
    expected_results = {'results': [(1, 2)], 'keys': ['column1','?column?']}
    results = pysoni_client_connection_with_envvars.execute_query(
        queryname="SELECT 1 AS column1, 2")
    
    assert expected_results == results

def test_execute_query_with_script(pysoni_client_connection_with_envvars):
    filename = 'execute_query'
    path = 'tests/fixtures/'

    expected_results = {'results': [(1, 2, 3)],
                'keys': ['?column?','?column?','?column?']}
    results_with_script = pysoni_client_connection_with_envvars.execute_query(
        queryname=filename, path_sql_script=path)

    assert expected_results == results_with_script

def test_execute_query_with_types(pysoni_client_connection_with_envvars):
        
    expected_results = {'results':[(1, 2, 3)], 
        'keys':['?column?','?column?','?column?'],
        'types':['int4', 'int4', 'int4']}
    results_with_types = pysoni_client_connection_with_envvars.execute_query(
        queryname="SELECT 1,2,3", types=True)

    assert expected_results == results_with_types