import pytest
import psycopg2


def test_postgre_statement(pysoni_client_connection_with_envvars):

    expected_result = "1"

    results = pysoni_client_connection_with_envvars.postgre_statement('select 1')

    results == expected_result
