import os

import pytest
import psycopg2
from dotenv import load_dotenv,find_dotenv

load_dotenv(find_dotenv())

def test_connection(pysoni_client, mocker):
    mocker.patch.object(psycopg2, 'connect')
    
    pysoni_client.connection()

    psycopg2.connect.assert_called_once_with(
        dbname='coverwalletdwh', user='cw_test', password='',
        host='localhost', port='5432',  options='-c statement_timeout=3600000')


def test_connection_with_connection_options(pysoni_client_with_timeout, mocker):
    mocker.patch.object(psycopg2, 'connect')

    pysoni_client_with_timeout.connection()

    psycopg2.connect.assert_called_once_with(
        dbname=os.environ['POSTGRES_DB'], user=os.environ['POSTGRES_USER'],
        password=os.environ.get('POSTGRES_PASSWORD', ''), host=os.environ.get('POSTGRES_HOST', 'localhost'),
        port=os.environ.get('POSTGRES_PORT', 5432), options='-c statement_timeout=1')

def test_invalid_connection(pysoni_invalid_client):

    with pytest.raises(psycopg2.DatabaseError):
        pysoni_invalid_client.connection()


def test_invalid_connection_with_options(pysoni_client_invalid_connection_options):

    with pytest.raises(psycopg2.OperationalError):
        pysoni_client_invalid_connection_options.connection()

def test_timeout_connection(pysoni_client_with_timeout):
    
    with pytest.raises(psycopg2.extensions.QueryCanceledError):
        cursor = pysoni_client_with_timeout.connection().cursor()
        cursor.execute('select pg_sleep(2000)')
