import pytest 
import psycopg2


def test_postgre_multiple_statement_with_psycopg2_api(pysoni_client, mocker):

    excepted_statements = """CREATE TABLE temp (cover text);ALTER TABLE 
                             temp rename to temp_prod; DROP TABLE temp_prod"""

    with mocker.patch.object(psycopg2, 'connect') as mock_connection:
        pysoni_client.postgre_multiple_statements(
            ["CREATE TABLE temp (cover text)", "ALTER TABLE temp RENAME TO temp_prod",
             "DROP TABLE temp_prod"])
        mock_connection.cursor.return_value.execute.called_with(excepted_statements)
