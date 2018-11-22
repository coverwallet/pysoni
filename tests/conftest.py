import os 
import pytest
from dotenv import load_dotenv,find_dotenv
from pysoni import Postgre

load_dotenv(find_dotenv())

@pytest.fixture
def pysoni_client():
    return Postgre(port='5432', host='localhost', user='cw_test', 
                   dbname='coverwalletdwh', password='')


@pytest.fixture
def pysoni_invalid_client():
    return Postgre(port='6000', host='localhost', user='cwtest',
                   dbname='coverwalletdwh', password='')

@pytest.fixture
def pysoni_client_connection_options():
    return Postgre(port='5432', host='localhost', user=os.environ['POSTGRES_USER'],
                   dbname=os.environ['POSTGRES_DB'], password='', 
                   connection_options='-c statement_timeout=1')


@pytest.fixture
def pysoni_client_invalid_connection_options():
    return Postgre(port='5432', host='localhost', user='cwtest',
                   dbname='coverwalletdwh', password='',
                   connection_options='-c statement_timeouts=1')
    
