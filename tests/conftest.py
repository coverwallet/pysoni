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
    
