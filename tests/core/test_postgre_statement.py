gitimport os
import pytest
import psycopg2
from dotenv import load_dotenv,find_dotenv

load_dotenv(find_dotenv())

def test_statement_integrity(pysoni_client_connection_with_envvars):

    assert pysoni_client_connection_with_envvars.postgre_statement("select 1")