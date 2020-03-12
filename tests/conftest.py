import os
import pytest
from dotenv import load_dotenv, find_dotenv
from pysoni import Postgre, Redshift
from pysoni.connection import Connection

load_dotenv(find_dotenv())


@pytest.fixture
def pysoni_client():
    return Postgre(port='5432', host='localhost', user='cw_test',
                   dbname='coverwalletdwh', password='')


@pytest.fixture
def pysoni_redshift_client():
    return Redshift(port='5439', host='amazon@ec2.com', user='cw_test',
                    dbname='coverwalletdwh', password='',
                    aws_access_key_id='aws_access_key_id',
                    aws_access_secret_key='aws_access_secret_key',
                    is_persistent=True)


@pytest.fixture()
def pysoni_redshift_client_not_persistent_connection():
    return Redshift(port='5439', host='amazon@ec2.com', user='cw_test',
                    dbname='coverwalletdwh', password='',
                    aws_access_key_id='aws_access_key_id',
                    aws_access_secret_key='aws_access_secret_key',
                    is_persistent=False)


@pytest.fixture
def pysoni_invalid_client():
    return Postgre(port='6000', host='localhost', user='cwtest',
                   dbname='coverwalletdwh', password='')


@pytest.fixture
def pysoni_client_invalid_connection_options():
    return Postgre(port='5432', host='localhost', user='cwtest',
                   dbname='coverwalletdwh', password='',
                   connection_options='-c statement_timeouts=1')


@pytest.fixture
def pysoni_client_connection_with_envvars():
    return Postgre(port=os.environ.get('POSTGRES_PORT', 5432), host=os.environ.get('POSTGRES_HOST', 'localhost'),
                   user=os.environ['POSTGRES_USER'], dbname=os.environ['POSTGRES_DB'],
                   password=os.environ.get('POSTGRES_PASSWORD', ''),
                   connection_options='-c statement_timeout=30000')


@pytest.fixture
def pysoni_client_with_timeout(pysoni_client_connection_with_envvars):
    pysoni_client_connection_with_envvars.db_connection.connection_options = '-c statement_timeout=1'
    return pysoni_client_connection_with_envvars


@pytest.fixture
def pysoni_client_persistent_connection_with_envvars(pysoni_client_connection_with_envvars):
    pysoni_client_connection_with_envvars.db_connection.is_persistent = True
    return pysoni_client_connection_with_envvars


@pytest.fixture
def connection():
    return Connection(port='5432', host='localhost', user='cw_test',
                      dbname='coverwalletdwh', password='', uri=None,
                      connection_options=None)


@pytest.fixture
def open_connection(connection, mocker):
    connection._connection_handler = mocker.Mock()
    connection._connection_handler.closed = 0

    return connection
