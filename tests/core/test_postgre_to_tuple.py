import pytest
from datetime import datetime
from dotenv import load_dotenv,find_dotenv

load_dotenv(find_dotenv())

def test_postgre_to_tuple(pysoni_client_connection_with_envvars):
    
    expected_results = [(1, datetime.date(datetime(2001, 9, 28)), 3)]
    results = pysoni_client_connection_with_envvars.postgre_to_tuple(
        query="SELECT 1 as mynum, date '2001-09-28' AS test_date, 3")
    
    assert expected_results == results