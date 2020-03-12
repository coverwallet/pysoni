import pytest
import psycopg2


def test_postgre_multiple_statement_with_psycopg2_api(pysoni_client_connection_with_envvars):

    excepted_statements = "1"

    results = pysoni_client_connection_with_envvars.postgre_multiple_statements(
        ["CREATE TABLE temp (cover text)", "INSERT INTO temp VALUES ('1')",
         "ALTER TABLE temp rename to temp_1", "SELECT * FROM temp_1"])

    excepted_statements == results
