import pytest 
import psycopg2
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

def test_postgre_multiple_statement_with_psycopg2_api(pysoni_client_connection_with_envvars, mocker):

    expected_result = "CREATE TABLE temp_testing (cover text)"

    pysoni_client_connection_with_envvars.postgre_statement(
        "CREATE TABLE temp_testing (cover text)")

    with mocker.patch.object(psycopg2, 'connect') as mock_connection:
        mock_connection.cursor.return_value.execute.called_with(
            expected_result
        )