import os
import pytest
import psycopg2
from datetime import datetime
from dotenv import load_dotenv,find_dotenv
from pandas import DataFrame, to_datetime
import numpy as np
import sys

load_dotenv(find_dotenv())

df = DataFrame(columns=["id", "name", "quantity"], 
    data=[[245, 'Peter', 31.2], [541, 'Lucas', np.nan]])

df1 = DataFrame(columns=["id", "name", "quantity"], 
    data=[[245, 'Will', 47.3]])

def test_dataframe_to_postgre_append(pysoni_client_connection_with_envvars):

    pysoni_client_connection_with_envvars.postgre_statement(
        "CREATE TABLE IF NOT EXISTS test_dataframe_to_postgre (id INT, name VARCHAR(50), \
        quantity float8);")
    
    pysoni_client_connection_with_envvars.dataframe_to_postgre(
        tablename="test_dataframe_to_postgre", dataframe_object=df, method='append',
        batch_size=1)

    expected_results = {'results': [(245, 'Peter', 31.2), (541, 'Lucas', None)],
        'keys': ['id', 'name', 'quantity']}
    results = pysoni_client_connection_with_envvars.execute_query(
        "SELECT * FROM test_dataframe_to_postgre")

    assert expected_results == results


def test_dataframe_to_postgre_rebuild(pysoni_client_connection_with_envvars):

    pysoni_client_connection_with_envvars.postgre_statement(
        "CREATE TABLE IF NOT EXISTS test_dataframe_to_postgre (id INT, \
        name VARCHAR(50), quantity float8);")

    pysoni_client_connection_with_envvars.dataframe_to_postgre(
        tablename="test_dataframe_to_postgre", dataframe_object=df1,
        method='rebuild', merge_key='id', batch_size=1)
    
    expected_results = {'results': [(541, 'Lucas', None), (245, 'Will', 47.3)],
        'keys': ['id', 'name', 'quantity']}
    results = pysoni_client_connection_with_envvars.execute_query(
        "SELECT * FROM test_dataframe_to_postgre")

    pysoni_client_connection_with_envvars.postgre_statement(
        "TRUNCATE TABLE test_dataframe_to_postgre;")

    assert expected_results == results


def test_dataframe_to_postgre_with_invalid_method(
    pysoni_client_connection_with_envvars):

    with pytest.raises(ValueError):
        pysoni_client_connection_with_envvars.dataframe_to_postgre(
            tablename="myTable", dataframe_object=df, method='apend',
            batch_size=1)


def test_dataframe_to_postgre_rebuilt_not_merge_key(
    pysoni_client_connection_with_envvars):

    with pytest.raises(ValueError):
        pysoni_client_connection_with_envvars.dataframe_to_postgre(
            tablename="myTable", dataframe_object=df, method='rebuild',
            batch_size=1)