import pytest 
import psycopg2

def test_postgre_statement_with_psycopg2_api(pysoni_client, mocker):
    
    expected_result = "CREATE TABLE temp_testing (cover text)"

    with mocker.patch.object(psycopg2, 'connect') as mock_connection:
        pysoni_client.postgre_statement(
            "CREATE TABLE temp_testing (cover text)")
        mock_connection.cursor.return_value.execute.called_with(expected_result)