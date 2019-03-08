import pytest 
import psycopg2
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

def test_postgre_statement_with_psycopg2_api(
        pysoni_client_connection_with_envvars, mocker):

    excepted_statements = """CREATE TABLE temp (cover text);ALTER TABLE 
                             temp rename to temp_prod; DROP TABLE temp_prod"""

    pysoni_client_connection_with_envvars.postgre_statement(
        ["CREATE TABLE temp (cover text)", "ALTER TABLE temp RENAME TO temp_prod",
         "DROP TABLE temp_prod"])

    with mocker.patch.object(psycopg2, 'connect') as mock_connection:
        mock_connection.cursor.return_value.execute.called_with(
            excepted_statements
        )