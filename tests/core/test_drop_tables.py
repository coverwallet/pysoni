import pytest


def test_drop_tables_executes_a_single_statement(pysoni_client, mocker):
    mocker.patch.object(pysoni_client, "connection")
    mocker.patch.object(pysoni_client, "postgre_statement")

    expected_statement = "DROP TABLES table1, table2, table3"

    pysoni_client.drop_tables(
      table_names=['table1', 'table2', 'table3'],
      wait_time=0,
      batch=True
    )

    pysoni_client.postgre_statement.assert_called_once_with(
      expected_statement,
      timesleep=0
    )


def test_drop_tables_with_batch_false_executes_multiple_statements(pysoni_client, mocker):
    mocker.patch.object(pysoni_client, "connection")
    mocker.patch.object(pysoni_client, "postgre_statement")
    
    expected_statement = "DROP TABLE table1"

    pysoni_client.drop_tables(
      table_names=['table1'],
      wait_time=0,
      batch=False
    )

    pysoni_client.postgre_statement.assert_called_once_with(
        expected_statement,
        timesleep=0
    )
